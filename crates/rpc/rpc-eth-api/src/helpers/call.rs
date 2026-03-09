//! Loads a pending block from database. Helper trait for `eth_` transaction, call and trace RPC
//! methods.

use core::fmt;

use super::{LoadBlock, LoadPendingBlock, LoadState, LoadTransaction, SpawnBlocking, Trace};
use crate::{
    helpers::estimate::EstimateCall, FromEvmError, FullEthApiTypes, RpcBlock, RpcNodeCore,
};
use alloy_consensus::{transaction::TxHashRef, BlockHeader};
use alloy_eips::eip2930::AccessListResult;
use alloy_evm::overrides::{apply_block_overrides, apply_state_overrides, OverrideBlockHashes};
use alloy_network::TransactionBuilder;
use alloy_primitives::{Bytes, B256, U256};
use alloy_rpc_types_eth::{
    simulate::{SimBlock, SimulatePayload, SimulatedBlock},
    state::{EvmOverrides, StateOverride},
    BlockId, Bundle, EthCallResponse, StateContext, TransactionInfo,
};
use futures::Future;
use reth_errors::{ProviderError, RethError};
use reth_evm::{
    env::BlockEnvironment, ConfigureEvm, Evm, EvmEnvFor, HaltReasonFor, InspectorFor,
    TransactionEnv, TxEnvFor,
};
use reth_node_api::BlockBody;
use reth_primitives_traits::Recovered;
use reth_revm::{
    cancelled::CancelOnDrop,
    database::StateProviderDatabase,
    db::{bal::EvmDatabaseError, State, CacheState},
};
use reth_rpc_convert::{RpcConvert, RpcTxReq};
use reth_rpc_eth_types::{
    cache::db::StateProviderTraitObjWrapper,
    error::{AsEthApiError, FromEthApiError},
    simulate::{self, EthSimulateError},
    EthApiError, StateCacheDb,
};
use reth_storage_api::{BlockIdReader, ProviderTx, StateProviderBox};
use reth_storage_errors::any::AnyError;
use revm::{
    context::{Block, DBErrorMarker},
    context_interface::{result::ResultAndState, Transaction},
    Database, DatabaseRef, DatabaseCommit,
    database::{CacheDB, async_db::DatabaseAsyncRef, WrapDatabaseAsync, WrapDatabaseRef, Cache, DbAccount, PlainAccount, AccountStatus, AccountState, BundleState, OriginalValuesKnown, RevertToSlot},
    bytecode::Bytecode,
    state::AccountInfo,
    database::states::CacheAccount,
};
use revm_inspectors::{access_list::AccessListInspector, transfer::TransferInspector};
use tracing::{trace, warn};
use crate::EthApiTypes;
use tokio::task::JoinError;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::sync::{RwLock, Mutex};
use std::collections::HashMap;
use std::pin::Pin;
use futures::{FutureExt, future::Shared};
use rocksdb::{DB, Options};
use std::path::Path;

use parking_lot::{Mutex as ParkingMutex, RwLock as ParkingRwLock};
use parking_lot::Condvar as ParkingCondvar;

use std::sync::{
    Arc,
    LazyLock,
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use alloy_primitives::{Address, Uint, FixedBytes};

// reusable executor closure
use rayon::prelude::*;

/// Result type for `eth_simulateV1` RPC method.
pub type SimulatedBlocksResult<N, E> = Result<Vec<SimulatedBlock<RpcBlock<N>>>, E>;

pub static GLOBAL_CACHE: LazyLock<Arc<ParkingRwLock<Cache>>> =
    LazyLock::new(|| Arc::new(ParkingRwLock::new(Cache::default())));

pub static GLOBAL_DB: OnceLock<Arc<RocksDBWrapper>> = OnceLock::new();

type SharedFut<T, E> =
    Shared<Pin<Box<dyn Future<Output = Result<T, E>> + Send>>>;


fn from_cache(cache: Cache) -> CacheState {
        let accounts = cache
            .accounts
            .into_iter()
            .map(|(address, db_account)| {
                let status = match db_account.account_state {
                    AccountState::NotExisting => AccountStatus::LoadedNotExisting,
                    AccountState::Touched => AccountStatus::Changed,
                    AccountState::StorageCleared => AccountStatus::Changed,
                    AccountState::None => AccountStatus::Loaded,
                };

                let account = match db_account.account_state {
                    AccountState::NotExisting => None,
                    _ => Some(PlainAccount {
                        info: db_account.info,
                        storage: db_account.storage,
                    }),
                };

                (
                    address,
                    CacheAccount {
                        account,
                        status,
                    },
                )
            })
            .collect();

        CacheState {
            accounts,
            contracts: cache.contracts,
            has_state_clear: false, // or derive from context
        }
}

fn into_cache(cache_state: CacheState) -> Cache {
    let accounts = cache_state
        .accounts
        .into_iter()
        .map(|(address, cache_account)| {
            let account_state = match cache_account.status {
                AccountStatus::LoadedNotExisting => AccountState::NotExisting,
                AccountStatus::Changed => AccountState::Touched,
                AccountStatus::Loaded => AccountState::None,
                AccountStatus::Destroyed => AccountState::NotExisting,
                _ => AccountState::None, // default to None for any other status
            };

            let (info, storage) = match cache_account.account {
                Some(plain) => (plain.info, plain.storage),
                None => (
                    AccountInfo::default(),
                    Default::default(),
                ),
            };

            (
                address,
                DbAccount {
                    info,
                    storage,
                    account_state,
                },
            )
        })
        .collect();

    Cache {
        accounts,
        contracts: cache_state.contracts,
        ..Default::default()
    }
}

use std::mem;

pub fn init_db(path: &str) -> eyre::Result<Arc<RocksDBWrapper>> {
    if let Some(existing) = GLOBAL_DB.get() {
        return Ok(existing.clone());
    }

    let db = Arc::new(RocksDBWrapper::open(path)?);

    // If another thread initialized it between our check and set,
    // set() will fail — so we recover gracefully.
    match GLOBAL_DB.set(db.clone()) {
        Ok(()) => Ok(db),
        Err(_) => Ok(GLOBAL_DB.get().unwrap().clone()),
    }
}

pub fn backfill() -> eyre::Result<()> {
    let db = init_db("./cache.db")?;

    let cache_snapshot = {
        let mut guard = GLOBAL_CACHE.write();
        mem::take(&mut *guard)
    };

    let state = from_cache(cache_snapshot);

    db.commit_cache(&state)?;

    Ok(())
}

pub fn load() -> eyre::Result<()> {
    let db = init_db("./cache.db")?;

    let cache_state = db.read_cache()?;

    let cache = into_cache(cache_state);

    let mut guard = GLOBAL_CACHE.write();
    *guard = cache;

    Ok(())
}


pub fn update(bundle: BundleState, revert: bool) -> eyre::Result<()> {

    let db = init_db("./cache.db")?;
    let mut cache = GLOBAL_CACHE
        .as_ref()
        .write();

    let mut batch = rocksdb::WriteBatch::default();

    if revert {
        let plain = bundle.reverts.to_plain_state_reverts();

        // =============================
        // ACCOUNTS (REVERT)
        // =============================
        for revert_block in plain.accounts {
            for (address, maybe_info) in revert_block {
                let addr_bytes = address.as_slice();

                let mut key = vec![b'a'];
                key.extend_from_slice(addr_bytes);

                match maybe_info {
                    Some(info) => {
                        cache.accounts
                            .entry(address)
                            .and_modify(|acc| {
                                acc.info = info.clone();
                                acc.account_state = AccountState::Touched;
                            })
                            .or_insert(DbAccount {
                                info: info.clone(),
                                account_state: AccountState::Touched,
                                storage: Default::default(),
                            });

                        batch.put(key, bincode::serialize(&info)?);
                    }
                    None => {
                        cache.accounts.remove(&address);
                        batch.delete(key);
                    }
                }
            }
        }

        // =============================
        // STORAGE (REVERT)
        // =============================
        for revert_block in plain.storage {
            for storage_revert in revert_block {
                let addr = storage_revert.address;
                let addr_bytes = addr.as_slice();

                if let Some(acc) = cache.accounts.get_mut(&addr) {
                    if storage_revert.wiped {
                        acc.storage.clear();
                    }
                }

                for (slot, revert_to) in storage_revert.storage_revert {
                    let slot_bytes = slot.to_be_bytes::<32>();

                    let mut key = vec![b's'];
                    key.extend_from_slice(addr_bytes);
                    key.extend_from_slice(&slot_bytes);

                    match revert_to {
                        RevertToSlot::Some(value) => {
                            cache.accounts
                                .entry(addr)
                                .or_insert(DbAccount {
                                    info: Default::default(),
                                    account_state: AccountState::Touched,
                                    storage: Default::default(),
                                })
                                .storage
                                .insert(slot, value);

                            batch.put(key, bincode::serialize(&value)?);
                        }
                        RevertToSlot::Destroyed => {
                            if let Some(acc) =
                                cache.accounts.get_mut(&addr)
                            {
                                acc.storage.remove(&slot);
                            }

                            batch.delete(key);
                        }
                    }
                }
            }
        }
    } else {
        let plain =
            bundle.to_plain_state(OriginalValuesKnown::Yes);

        // =============================
        // ACCOUNTS (FORWARD)
        // =============================
        for (address, maybe_info) in plain.accounts {
            let addr_bytes = address.as_slice();

            let mut key = vec![b'a'];
            key.extend_from_slice(addr_bytes);

            match maybe_info {
                Some(info) => {
                    cache.accounts
                        .entry(address)
                        .and_modify(|acc| {
                            acc.info = info.clone();
                            acc.account_state = AccountState::Touched;
                        })
                        .or_insert(DbAccount {
                            info: info.clone(),
                            account_state: AccountState::Touched,
                            storage: Default::default(),
                        });

                    batch.put(key, bincode::serialize(&info)?);
                }
                None => {
                    cache.accounts.remove(&address);
                    batch.delete(key);
                }
            }
        }

        // =============================
        // STORAGE (FORWARD)
        // =============================
        for storage_change in plain.storage {
            let addr = storage_change.address;
            let addr_bytes = addr.as_slice();

            if let Some(acc) = cache.accounts.get_mut(&addr) {
                if storage_change.wipe_storage {
                    acc.storage.clear();
                }
            }

            for (slot, value) in storage_change.storage {
                let slot_bytes = slot.to_be_bytes::<32>();

                let mut key = vec![b's'];
                key.extend_from_slice(addr_bytes);
                key.extend_from_slice(&slot_bytes);

                cache.accounts
                    .entry(addr)
                    .or_insert(DbAccount {
                        info: Default::default(),
                        account_state: AccountState::Touched,
                        storage: Default::default(),
                    })
                    .storage
                    .insert(slot, value);

                batch.put(key, bincode::serialize(&value)?);
            }
        }

        // =============================
        // CONTRACTS
        // =============================
        for (code_hash, bytecode) in plain.contracts {
            if bytecode.is_empty() {
                continue;
            }

            cache.contracts.insert(code_hash, bytecode.clone());

            let mut key = vec![b'c'];
            key.extend_from_slice(code_hash.as_slice());

            batch.put(key, bincode::serialize(&bytecode)?);
        }
    }

    db.db.write(batch)?;

    Ok(())
}


#[derive(Debug)]
struct Flight<T, E> {
    result: ParkingMutex<Option<Result<T, E>>>,
    ready: ParkingCondvar,
}

impl<T: Clone, E: Clone> Flight<T, E> {
    fn new() -> Self {
        Self {
            result: ParkingMutex::new(None),
            ready: ParkingCondvar::new(),
        }
    }

    fn complete(&self, value: Result<T, E>) {
        *self.result.lock() = Some(value);
        self.ready.notify_all();
    }

    fn wait(&self) -> Result<T, E> {
        let mut guard = self.result.lock();
        while guard.is_none() {
            self.ready.wait(&mut guard);
        }
        guard.clone().unwrap()
    }
}



#[derive(Debug)]
pub struct CacheDBParking<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: Clone,
{
    db: Arc<ExtDB>,
    cache: Arc<ParkingRwLock<Cache>>,

    inflight_basic: Arc<
        ParkingMutex<HashMap<Address, Arc<Flight<Option<AccountInfo>, ExtDB::Error>>>>
    >,

    inflight_code: Arc<
        ParkingMutex<HashMap<B256, Arc<Flight<Bytecode, ExtDB::Error>>>>
    >,

    inflight_storage: Arc<
        ParkingMutex<HashMap<(Address, U256), Arc<Flight<U256, ExtDB::Error>>>>
    >,

    inflight_block: Arc<
        ParkingMutex<HashMap<u64, Arc<Flight<B256, ExtDB::Error>>>>
    >,
}

impl<ExtDB> CacheDBParking<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: Clone,
{
    pub fn new(db: ExtDB) -> Self {
        Self {
            db: Arc::new(db),
            cache: Arc::new(ParkingRwLock::new(Cache::default())),
            inflight_basic: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_code: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_storage: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_block: Arc::new(ParkingMutex::new(HashMap::new())),
        }
    }
}

impl<ExtDB> DatabaseRef for CacheDBParking<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: Clone,
{
    type Error = ExtDB::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(acc) = self.cache.read().accounts.get(&address) {
            return Ok(acc.info());
        }

        let flight = {
            let mut map = self.inflight_basic.lock();

            if let Some(f) = map.get(&address) {
                return f.wait();
            }

            let f = Arc::new(Flight::new());
            map.insert(address, f.clone());
            f
        };

        let result = self.db.basic_ref(address);

        if let Ok(Some(ref info)) = result {
            let mut w = self.cache.write();
            w.accounts.insert(
                address,
                DbAccount {
                    info: info.clone(),
                    ..Default::default()
                },
            );
        }

        flight.complete(result.clone());
        self.inflight_basic.lock().remove(&address);

        result
    }

    fn code_by_hash_ref(
        &self,
        code_hash: B256,
    ) -> Result<Bytecode, Self::Error> {
        if let Some(code) = self.cache.read().contracts.get(&code_hash) {
            return Ok(code.clone());
        }

        let flight = {
            let mut map = self.inflight_code.lock();

            if let Some(f) = map.get(&code_hash) {
                return f.wait();
            }

            let f = Arc::new(Flight::new());
            map.insert(code_hash, f.clone());
            f
        };

        let result = self.db.code_by_hash_ref(code_hash);

        if let Ok(ref code) = result {
            self.cache.write().contracts.insert(code_hash, code.clone());
        }

        flight.complete(result.clone());
        self.inflight_code.lock().remove(&code_hash);

        result
    }

    fn storage_ref(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        if let Some(acc) = self.cache.read().accounts.get(&address) {
            if let Some(v) = acc.storage.get(&index) {
                return Ok(*v);
            }
        }

        let key = (address, index);

        let flight = {
            let mut map = self.inflight_storage.lock();

            if let Some(f) = map.get(&key) {
                return f.wait();
            }

            let f = Arc::new(Flight::new());
            map.insert(key, f.clone());
            f
        };

        let result = self.db.storage_ref(address, index);

        if let Ok(value) = result {
            let mut w = self.cache.write();
            let acc = w.accounts.entry(address).or_default();
            acc.storage.insert(index, value);
        }

        flight.complete(result.clone());
        self.inflight_storage.lock().remove(&key);

        result
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let key = U256::from(number);

        if let Some(hash) = self.cache.read().block_hashes.get(&key) {
            return Ok(*hash);
        }

        let flight = {
            let mut map = self.inflight_block.lock();

            if let Some(f) = map.get(&number) {
                return f.wait();
            }

            let f = Arc::new(Flight::new());
            map.insert(number, f.clone());
            f
        };

        let result = self.db.block_hash_ref(number);

        if let Ok(hash) = result {
            self.cache.write().block_hashes.insert(key, hash);
        }

        flight.complete(result.clone());
        self.inflight_block.lock().remove(&number);

        result
    }
}

pub struct RocksDBWrapper {
    db: Arc<DB>,
}

impl RocksDBWrapper {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, path)?;
        
        Ok(Self {
            db: Arc::new(db),
        })
    }

pub fn commit_cache(&self, cache: &CacheState) -> Result<(), AnyError> {
    const MAX_BATCH_KV: usize = 200_000;

    // Collect all KV pairs
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    // -------------------------
    // Contracts
    // -------------------------
    for (code_hash, bytecode) in &cache.contracts {
        if bytecode.is_empty() {
            continue;
        }

        let mut key = Vec::with_capacity(1 + 32);
        key.push(b'c');
        key.extend_from_slice(code_hash.as_slice());

        let value =
            bincode::serialize(bytecode)
                .map_err(AnyError::new)?;

        entries.push((key, value));
    }

    // -------------------------
    // Accounts
    // -------------------------
    for (address, cache_account) in &cache.accounts {
        let addr_bytes = address.as_slice();

        // Destroyed
        if cache_account.status.was_destroyed() {
            let mut key = Vec::with_capacity(1 + addr_bytes.len());
            key.push(b'a');
            key.extend_from_slice(addr_bytes);

            // empty value means delete marker
            entries.push((key, Vec::new()));
            continue;
        }

        let Some(plain_account) = &cache_account.account else {
            continue;
        };

        // AccountInfo
        {
            let mut key = Vec::with_capacity(1 + addr_bytes.len());
            key.push(b'a');
            key.extend_from_slice(addr_bytes);

            let value =
                bincode::serialize(&plain_account.info)
                    .map_err(AnyError::new)?;

            entries.push((key, value));
        }

        // Storage
        for (slot, value) in &plain_account.storage {
            let slot_bytes = slot.to_be_bytes::<32>();

            let mut key =
                Vec::with_capacity(1 + addr_bytes.len() + 32);

            key.push(b's');
            key.extend_from_slice(addr_bytes);
            key.extend_from_slice(&slot_bytes);

            let val =
                bincode::serialize(value)
                    .map_err(AnyError::new)?;

            entries.push((key, val));
        }

        // Inline code
        if let Some(code) = &plain_account.info.code {
            if !code.is_empty() {
                let mut key = Vec::with_capacity(1 + 32);
                key.push(b'c');
                key.extend_from_slice(
                    plain_account.info.code_hash.as_slice(),
                );

                let val =
                    bincode::serialize(code)
                        .map_err(AnyError::new)?;

                entries.push((key, val));
            }
        }
    }

    // -------------------------
    // Sort by raw key bytes
    // -------------------------
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // -------------------------
    // Write in large KV batches
// -------------------------
    let mut batch = rocksdb::WriteBatch::default();
    let mut kv_count = 0;

    for (key, value) in entries {
        if value.is_empty() {
            batch.delete(key);
        } else {
            batch.put(key, value);
        }

        kv_count += 1;

        if kv_count >= MAX_BATCH_KV {
            self.db.write(batch).map_err(AnyError::new)?;
            batch = rocksdb::WriteBatch::default();
            kv_count = 0;
        }
    }

    self.db.write(batch).map_err(AnyError::new)?;
    Ok(())
}


    

pub fn read_cache(&self) -> Result<CacheState, AnyError> {
    use rocksdb::IteratorMode;

    let mut cache = CacheState {
        accounts: HashMap::default(),
        contracts: HashMap::default(),
        has_state_clear: false,
    };

    let iter = self.db.iterator(IteratorMode::Start);

    for item in iter {
        let (key, value) = item.map_err(AnyError::new)?;

        if key.is_empty() {
            continue;
        }

        match key[0] {
            // -------------------------
            // Account
            // -------------------------
            b'a' => {
                // address is 20 bytes after prefix
                if key.len() != 1 + 20 {
                    continue;
                }

                let address = Address::from_slice(&key[1..21]);

                // deletion marker
                if value.is_empty() {
                    cache.accounts.insert(
                        address,
                        CacheAccount {
                            account: None,
                            status: AccountStatus::Destroyed,
                        },
                    );
                    continue;
                }

                let info: AccountInfo =
                    bincode::deserialize(&value)
                        .map_err(AnyError::new)?;

                let entry = cache.accounts.entry(address).or_insert(
                    CacheAccount {
                        account: Some(PlainAccount {
                            info: info.clone(),
                            storage: HashMap::default(),
                        }),
                        status: AccountStatus::Loaded,
                    },
                );

                if let Some(account) = &mut entry.account {
                    account.info = info;
                }
            }

            // -------------------------
            // Storage
            // -------------------------
            b's' => {
                // key = 1 + 20 + 32
                if key.len() != 1 + 20 + 32 {
                    continue;
                }

                let address = Address::from_slice(&key[1..21]);
                let slot_bytes = &key[21..53];

                // Explicit 32-byte conversion
                let bytes: [u8; 32] =
                    slot_bytes.try_into().map_err(AnyError::new)?;

                let slot = Uint::<256, 4>::from_be_bytes(bytes);

                let value =
                    bincode::deserialize(&value)
                        .map_err(AnyError::new)?;

                let entry = cache.accounts.entry(address).or_insert(
                    CacheAccount {
                        account: Some(PlainAccount {
                            info: AccountInfo::default(),
                            storage: HashMap::default(),
                        }),
                        status: AccountStatus::Loaded,
                    },
                );

                if let Some(account) = &mut entry.account {
                    account.storage.insert(slot, value);
                }
            }

            // -------------------------
            // Contract
            // -------------------------
            b'c' => {
                if key.len() != 1 + 32 {
                    continue;
                }

                let code_hash =
                    FixedBytes::<32>::from_slice(&key[1..33]);

                let code: Bytecode =
                    bincode::deserialize(&value)
                        .map_err(AnyError::new)?;

                cache.contracts.insert(code_hash, code);
            }

            _ => {}
        }
    }

    Ok(cache)
}

}

impl Database for RocksDBWrapper {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key = format!("acct:{:?}", address);

        match self.db.get(key).map_err(AnyError::new)? {
            Some(bytes) => {
                let info: AccountInfo =
                    bincode::deserialize(&bytes).map_err(AnyError::new)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    fn code_by_hash(&mut self, hash: B256) -> Result<Bytecode, Self::Error> {
        let key = format!("code:{:?}", hash);

        match self.db.get(key).map_err(AnyError::new)? {
            Some(bytes) => {
                let code: Bytecode =
                    bincode::deserialize(&bytes).map_err(AnyError::new)?;
                Ok(code)
            }
            None => Ok(Bytecode::default()),
        }
    }

    fn storage(&mut self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        let key = format!("storage:{:?}:{:?}", address, slot);

        match self.db.get(key).map_err(AnyError::new)? {
            Some(bytes) => {
                let value: U256 =
                    bincode::deserialize(&bytes).map_err(AnyError::new)?;
                Ok(value)
            }
            None => Ok(U256::ZERO),
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let key = format!("blockhash:{:?}", number);

        match self.db.get(key).map_err(AnyError::new)? {
            Some(bytes) => {
                let hash: B256 =
                    bincode::deserialize(&bytes).map_err(AnyError::new)?;
                Ok(hash)
            }
            None => Ok(B256::ZERO),
        }
    }
}




// Per-key entry
type Entry<V, E> = Arc<ParkingRwLock<Option<Result<V, E>>>>;

#[derive(Debug)]
pub struct CacheDBRwLock<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync ,
    ExtDB::Error: Clone,
{
    db: Arc<TimedDB<ExtDB>>,
    cache: Arc<ParkingRwLock<Cache>>,

    inflight_basic: Arc<
        ParkingMutex<HashMap<Address, Entry<Option<AccountInfo>, ExtDB::Error>>>,
    >,

    inflight_code: Arc<
        ParkingMutex<HashMap<B256, Entry<Bytecode, ExtDB::Error>>>,
    >,

    inflight_storage: Arc<
        ParkingMutex<HashMap<(Address, U256), Entry<U256, ExtDB::Error>>>,
    >,

    inflight_block: Arc<
        ParkingMutex<HashMap<u64, Entry<B256, ExtDB::Error>>>,
    >,
}

impl<ExtDB> Clone for CacheDBRwLock<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync ,
    ExtDB::Error: Clone,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            cache: self.cache.clone(),
            inflight_basic: self.inflight_basic.clone(),
            inflight_code: self.inflight_code.clone(),
            inflight_storage: self.inflight_storage.clone(),
            inflight_block: self.inflight_block.clone(),
        }
    }
}

impl<ExtDB> CacheDBRwLock<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync ,
    ExtDB::Error: Clone,
{
    pub fn new(db: ExtDB) -> Self {
        Self {
            db: Arc::new(TimedDB::new(db)),
            cache: Arc::new(ParkingRwLock::new(Cache::default())),
            inflight_basic: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_code: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_storage: Arc::new(ParkingMutex::new(HashMap::new())),
            inflight_block: Arc::new(ParkingMutex::new(HashMap::new())),
        }
    }

    pub fn print_and_reset(&self) {
        self.db.print_and_reset();
    }
}

impl<ExtDB> DatabaseRef for CacheDBRwLock<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync ,
    ExtDB::Error: Clone,
{
    type Error = ExtDB::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {

        // fast cache check
        if let Some(acc) = self.cache.read().accounts.get(&address) {
            return Ok(acc.info());
        }

        // get per-key entry
        let entry = {
            let mut map = self.inflight_basic.lock();
            map.entry(address)
                .or_insert_with(|| Arc::new(ParkingRwLock::new(None)))
                .clone()
        };

        // fast read path
        {
            let guard = entry.read();
            if let Some(result) = &*guard {
                return result.clone();
            }
        }

        // write path (single fetcher)
        let mut guard = entry.write();

        // double-check
        if let Some(result) = &*guard {
            return result.clone();
        }

        // fetch
        let result = self.db.basic_ref(address);

        // store cache
        if let Ok(Some(ref info)) = result {
            let mut cache = self.cache.write();
            cache.accounts.insert(
                address,
                DbAccount {
                    info: info.clone(),
                    ..Default::default()
                },
            );
        }

        // publish result
        *guard = Some(result.clone());

        result
    }

    fn code_by_hash_ref(
        &self,
        code_hash: B256,
    ) -> Result<Bytecode, Self::Error> {

        if let Some(code) = self.cache.read().contracts.get(&code_hash) {
            return Ok(code.clone());
        }

        let entry = {
            let mut map = self.inflight_code.lock();
            map.entry(code_hash)
                .or_insert_with(|| Arc::new(ParkingRwLock::new(None)))
                .clone()
        };

        {
            let guard = entry.read();
            if let Some(result) = &*guard {
                return result.clone();
            }
        }

        let mut guard = entry.write();

        if let Some(result) = &*guard {
            return result.clone();
        }

        let result = self.db.code_by_hash_ref(code_hash);

        if let Ok(ref code) = result {
            self.cache.write().contracts.insert(code_hash, code.clone());
        }

        *guard = Some(result.clone());

        result
    }

    fn storage_ref(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, Self::Error> {

        if let Some(acc) = self.cache.read().accounts.get(&address) {
            if let Some(v) = acc.storage.get(&index) {
                return Ok(*v);
            }
        }

        let key = (address, index);

        let entry = {
            let mut map = self.inflight_storage.lock();
            map.entry(key)
                .or_insert_with(|| Arc::new(ParkingRwLock::new(None)))
                .clone()
        };

        {
            let guard = entry.read();
            if let Some(result) = &*guard {
                return result.clone();
            }
        }

        let mut guard = entry.write();

        if let Some(result) = &*guard {
            return result.clone();
        }

        let result = self.db.storage_ref(address, index);

        if let Ok(value) = result {
            let mut cache = self.cache.write();
            let acc = cache.accounts.entry(address).or_default();
            acc.storage.insert(index, value);
        }

        *guard = Some(result.clone());

        result
    }

    fn block_hash_ref(
        &self,
        number: u64,
    ) -> Result<B256, Self::Error> {

        let key = U256::from(number);

        if let Some(hash) = self.cache.read().block_hashes.get(&key) {
            return Ok(*hash);
        }

        let entry = {
            let mut map = self.inflight_block.lock();
            map.entry(number)
                .or_insert_with(|| Arc::new(ParkingRwLock::new(None)))
                .clone()
        };

        {
            let guard = entry.read();
            if let Some(result) = &*guard {
                return result.clone();
            }
        }

        let mut guard = entry.write();

        if let Some(result) = &*guard {
            return result.clone();
        }

        let result = self.db.block_hash_ref(number);

        if let Ok(hash) = result {
            self.cache.write().block_hashes.insert(key, hash);
        }

        *guard = Some(result.clone());

        result
    }
}


#[derive(Debug)]
pub struct TimedDB<ExtDB>
where
    ExtDB: DatabaseRef,
{
    inner: Arc<ExtDB>,
    total_ns: Arc<AtomicU64>,
}

impl<ExtDB> TimedDB<ExtDB>
where
    ExtDB: DatabaseRef,
{
    pub fn new(db: ExtDB) -> Self {
        Self {
            inner: Arc::new(db),
            total_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_time<R>(&self, f: impl FnOnce(&ExtDB) -> R) -> R {
        let start = Instant::now();
        let result = f(&self.inner);
        let elapsed = start.elapsed().as_nanos() as u64;

        self.total_ns.fetch_add(elapsed, Ordering::Relaxed);

        result
    }

    fn print_and_reset(&self) {
        let ns = self.total_ns.swap(0, Ordering::Relaxed);
        tracing::info!(
            "Total DB fetch time: {:.3} ms",
            ns as f64 / 1_000_000.0
        );
    }
}

impl<ExtDB> Clone for TimedDB<ExtDB>
where
    ExtDB: DatabaseRef,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            total_ns: self.total_ns.clone(),
        }
    }
}

impl<ExtDB> DatabaseRef for TimedDB<ExtDB>
where
    ExtDB: DatabaseRef,
{
    type Error = ExtDB::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        self.record_time(|db| db.basic_ref(address))
    }

    fn code_by_hash_ref(
        &self,
        code_hash: B256,
    ) -> Result<Bytecode, Self::Error> {
        self.record_time(|db| db.code_by_hash_ref(code_hash))
    }

    fn storage_ref(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        self.record_time(|db| db.storage_ref(address, index))
    }

    fn block_hash_ref(
        &self,
        number: u64,
    ) -> Result<B256, Self::Error> {
        self.record_time(|db| db.block_hash_ref(number))
    }
}



#[derive(Debug)]
pub struct AsyncCacheDB<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: DBErrorMarker,
{
    pub cache: Arc<RwLock<Cache>>,
    pub db: Arc<ExtDB>,

    inflight_basic: Arc<
        Mutex<HashMap<Address, SharedFut<Option<AccountInfo>, ExtDB::Error>>>
    >,

    inflight_code: Arc<
        Mutex<HashMap<B256, SharedFut<Bytecode, ExtDB::Error>>>
    >,

    inflight_storage: Arc<
        Mutex<HashMap<(Address, U256), SharedFut<U256, ExtDB::Error>>>
    >,

    inflight_block: Arc<
        Mutex<HashMap<u64, SharedFut<B256, ExtDB::Error>>>
    >,
}

impl<ExtDB> Clone for CacheDBParking<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: Clone,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            cache: self.cache.clone(),
            inflight_basic: self.inflight_basic.clone(),
            inflight_code: self.inflight_code.clone(),
            inflight_storage: self.inflight_storage.clone(),
            inflight_block: self.inflight_block.clone(),
        }
    }
}



impl<ExtDB> AsyncCacheDB<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: DBErrorMarker + Clone,
{
    pub fn new(db: ExtDB) -> Self {
        Self {
            cache: Arc::new(RwLock::new(Cache::default())),
            db: Arc::new(db),
            inflight_basic: Arc::new(Mutex::new(HashMap::new())),
            inflight_code: Arc::new(Mutex::new(HashMap::new())),
            inflight_storage: Arc::new(Mutex::new(HashMap::new())),
            inflight_block: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}


impl<ExtDB> Clone for AsyncCacheDB<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: DBErrorMarker + Clone,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            db: self.db.clone(),
            inflight_basic: self.inflight_basic.clone(),
            inflight_code: self.inflight_code.clone(),
            inflight_storage: self.inflight_storage.clone(),
            inflight_block: self.inflight_block.clone(),
        }
    }
}


impl<ExtDB> DatabaseAsyncRef for AsyncCacheDB<ExtDB>
where
    ExtDB: DatabaseRef + Send + Sync + 'static,
    ExtDB::Error: DBErrorMarker + Clone,
{
    type Error = ExtDB::Error;

    fn basic_async_ref(
        &self,
        address: Address,
    ) -> impl Future<Output = Result<Option<AccountInfo>, Self::Error>> + Send {
        let cache = self.cache.clone();
        let db = self.db.clone();
        let inflight = self.inflight_basic.clone();

        async move {
            if let Some(acc) = cache.read().await.accounts.get(&address) {
                return Ok(acc.info());
            }

            let mut map = inflight.lock().await;

            if let Some(f) = map.get(&address) {
                return f.clone().await;
            }

            let fut = async move {
                let result = db.basic_ref(address)?;

                if let Some(ref info) = result {
                    let mut w = cache.write().await;
                    w.accounts.insert(
                        address,
                        DbAccount {
                            info: info.clone(),
                            ..Default::default()
                        },
                    );
                }

                Ok(result)
            }
            .boxed()
            .shared();

            map.insert(address, fut.clone());
            drop(map);

            let res = fut.await;
            inflight.lock().await.remove(&address);

            res
        }
    }

    fn code_by_hash_async_ref(
        &self,
        code_hash: B256,
    ) -> impl Future<Output = Result<Bytecode, Self::Error>> + Send {
        let cache = self.cache.clone();
        let db = self.db.clone();
        let inflight = self.inflight_code.clone();

        async move {
            if let Some(code) = cache.read().await.contracts.get(&code_hash) {
                return Ok(code.clone());
            }

            let mut map = inflight.lock().await;

            if let Some(f) = map.get(&code_hash) {
                return f.clone().await;
            }

            let fut = async move {
                let code = db.code_by_hash_ref(code_hash)?;

                let mut w = cache.write().await;
                w.contracts.insert(code_hash, code.clone());

                Ok(code)
            }
            .boxed()
            .shared();

            map.insert(code_hash, fut.clone());
            drop(map);

            let res = fut.await;
            inflight.lock().await.remove(&code_hash);

            res
        }
    }

    fn storage_async_ref(
        &self,
        address: Address,
        index: U256,
    ) -> impl Future<Output = Result<U256, Self::Error>> + Send {
        let cache = self.cache.clone();
        let db = self.db.clone();
        let inflight = self.inflight_storage.clone();

        async move {
            if let Some(acc) = cache.read().await.accounts.get(&address) {
                if let Some(v) = acc.storage.get(&index) {
                    return Ok(*v);
                }
            }

            let key = (address, index);
            let mut map = inflight.lock().await;

            if let Some(f) = map.get(&key) {
                return f.clone().await;
            }

            let fut = async move {
                let value = db.storage_ref(address, index)?;

                let mut w = cache.write().await;
                let acc = w.accounts.entry(address).or_default();
                acc.storage.insert(index, value);

                Ok(value)
            }
            .boxed()
            .shared();

            map.insert(key, fut.clone());
            drop(map);

            let res = fut.await;
            inflight.lock().await.remove(&key);

            res
        }
    }

    fn block_hash_async_ref(
        &self,
        number: u64,
    ) -> impl Future<Output = Result<B256, Self::Error>> + Send {
        let cache = self.cache.clone();
        let db = self.db.clone();
        let inflight = self.inflight_block.clone();

        async move {
            let key = U256::from(number);

            if let Some(hash) = cache.read().await.block_hashes.get(&key) {
                return Ok(*hash);
            }

            let mut map = inflight.lock().await;

            if let Some(f) = map.get(&number) {
                return f.clone().await;
            }

            let fut = async move {
                let hash = db.block_hash_ref(number)?;

                let mut w = cache.write().await;
                w.block_hashes.insert(key, hash);

                Ok(hash)
            }
            .boxed()
            .shared();

            map.insert(number, fut.clone());
            drop(map);

            let res = fut.await;
            inflight.lock().await.remove(&number);

            res
        }
    }
}



/// Execution related functions for the [`EthApiServer`](crate::EthApiServer) trait in
/// the `eth_` namespace.
pub trait EthCall: EstimateCall + Call + LoadPendingBlock + LoadBlock + FullEthApiTypes {
    /// Estimate gas needed for execution of the `request` at the [`BlockId`].
    fn estimate_gas_at(
        &self,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        at: BlockId,
        state_override: Option<StateOverride>,
    ) -> impl Future<Output = Result<U256, Self::Error>> + Send {
        EstimateCall::estimate_gas_at(self, request, at, state_override)
    }

    /// `eth_simulateV1` executes an arbitrary number of transactions on top of the requested state.
    /// The transactions are packed into individual blocks. Overrides can be provided.
    ///
    /// See also: <https://github.com/ethereum/go-ethereum/pull/27720>
    fn simulate_v1(
        &self,
        payload: SimulatePayload<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>,
        block: Option<BlockId>,
    ) -> impl Future<Output = SimulatedBlocksResult<Self::NetworkTypes, Self::Error>> + Send {
        async move {
            if payload.block_state_calls.len() > self.max_simulate_blocks() as usize {
                return Err(EthApiError::InvalidParams("too many blocks.".to_string()).into())
            }

            let block = block.unwrap_or_default();

            let SimulatePayload {
                block_state_calls,
                trace_transfers,
                validation,
                return_full_transactions,
            } = payload;

            if block_state_calls.is_empty() {
                return Err(EthApiError::InvalidParams(String::from("calls are empty.")).into())
            }

            let base_block =
                self.recovered_block(block).await?.ok_or(EthApiError::HeaderNotFound(block))?;
            let mut parent = base_block.sealed_header().clone();

            self.spawn_with_state_at_block(block, move |this, mut db| {
                let mut blocks: Vec<SimulatedBlock<RpcBlock<Self::NetworkTypes>>> =
                    Vec::with_capacity(block_state_calls.len());
                for block in block_state_calls {
                    let mut evm_env = this
                        .evm_config()
                        .next_evm_env(&parent, &this.next_env_attributes(&parent)?)
                        .map_err(RethError::other)
                        .map_err(Self::Error::from_eth_err)?;

                    // Always disable EIP-3607
                    evm_env.cfg_env.disable_eip3607 = true;

                    if !validation {
                        // If not explicitly required, we disable nonce check <https://github.com/paradigmxyz/reth/issues/16108>
                        evm_env.cfg_env.disable_nonce_check = true;
                        evm_env.cfg_env.disable_base_fee = true;
                        evm_env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);
                        evm_env.block_env.inner_mut().basefee = 0;
                    }

                    let SimBlock { block_overrides, state_overrides, calls } = block;

                    if let Some(block_overrides) = block_overrides {
                        // ensure we don't allow uncapped gas limit per block
                        if let Some(gas_limit_override) = block_overrides.gas_limit &&
                            gas_limit_override > evm_env.block_env.gas_limit() &&
                            gas_limit_override > this.call_gas_limit()
                        {
                            return Err(EthApiError::other(EthSimulateError::GasLimitReached).into())
                        }
                        apply_block_overrides(
                            block_overrides,
                            &mut db,
                            evm_env.block_env.inner_mut(),
                        );
                    }
                    if let Some(state_overrides) = state_overrides {
                        apply_state_overrides(state_overrides, &mut db)
                            .map_err(Self::Error::from_eth_err)?;
                    }

                    let block_gas_limit = evm_env.block_env.gas_limit();
                    let chain_id = evm_env.cfg_env.chain_id;

                    let default_gas_limit = {
                        let total_specified_gas =
                            calls.iter().filter_map(|tx| tx.as_ref().gas_limit()).sum::<u64>();
                        let txs_without_gas_limit =
                            calls.iter().filter(|tx| tx.as_ref().gas_limit().is_none()).count();

                        if total_specified_gas > block_gas_limit {
                            return Err(EthApiError::Other(Box::new(
                                EthSimulateError::BlockGasLimitExceeded,
                            ))
                            .into())
                        }

                        if txs_without_gas_limit > 0 {
                            (block_gas_limit - total_specified_gas) / txs_without_gas_limit as u64
                        } else {
                            0
                        }
                    };

                    let ctx = this
                        .evm_config()
                        .context_for_next_block(&parent, this.next_env_attributes(&parent)?)
                        .map_err(RethError::other)
                        .map_err(Self::Error::from_eth_err)?;
                    let map_err = |e: EthApiError| -> Self::Error {
                        match e.as_simulate_error() {
                            Some(sim_err) => Self::Error::from_eth_err(EthApiError::other(sim_err)),
                            None => Self::Error::from_eth_err(e),
                        }
                    };

                    let (result, results) = if trace_transfers {
                        // prepare inspector to capture transfer inside the evm so they are recorded
                        // and included in logs
                        let inspector = TransferInspector::new(false).with_logs(true);
                        let evm = this
                            .evm_config()
                            .evm_with_env_and_inspector(&mut db, evm_env, inspector);
                        let builder = this.evm_config().create_block_builder(evm, &parent, ctx);
                        simulate::execute_transactions(
                            builder,
                            calls,
                            default_gas_limit,
                            chain_id,
                            this.converter(),
                        )
                        .map_err(map_err)?
                    } else {
                        let evm = this.evm_config().evm_with_env(&mut db, evm_env);
                        let builder = this.evm_config().create_block_builder(evm, &parent, ctx);
                        simulate::execute_transactions(
                            builder,
                            calls,
                            default_gas_limit,
                            chain_id,
                            this.converter(),
                        )
                        .map_err(map_err)?
                    };

                    parent = result.block.clone_sealed_header();

                    let block = simulate::build_simulated_block::<Self::Error, _>(
                        result.block,
                        results,
                        return_full_transactions.into(),
                        this.converter(),
                    )?;

                    blocks.push(block);
                }

                Ok(blocks)
            })
            .await
        }
    }

    /// Executes the call request (`eth_call`) and returns the output
    fn call(
        &self,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        block_number: Option<BlockId>,
        overrides: EvmOverrides,
    ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send {
        async move {
            let _permit = self.acquire_owned_blocking_io().await;
            let res =
                self.transact_call_at(request, block_number.unwrap_or_default(), overrides).await?;

            Self::Error::ensure_success(res.result)
        }
    }

    /// Simulate arbitrary number of transactions at an arbitrary blockchain index, with the
    /// optionality of state overrides
    fn call_many(
        &self,
        bundles: Vec<Bundle<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>>,
        state_context: Option<StateContext>,
        mut state_override: Option<StateOverride>,
    ) -> impl Future<Output = Result<Vec<Vec<EthCallResponse>>, Self::Error>> + Send {
        async move {
            // Check if the vector of bundles is empty
            if bundles.is_empty() {
                return Err(EthApiError::InvalidParams(String::from("bundles are empty.")).into());
            }

            let StateContext { transaction_index, block_number } =
                state_context.unwrap_or_default();
            let transaction_index = transaction_index.unwrap_or_default();

            let mut target_block = block_number.unwrap_or_default();
            let is_block_target_pending = target_block.is_pending();

            // if it's not pending, we should always use block_hash over block_number to ensure that
            // different provider calls query data related to the same block.
            if !is_block_target_pending {
                target_block = self
                    .provider()
                    .block_hash_for_id(target_block)
                    .map_err(|_| EthApiError::HeaderNotFound(target_block))?
                    .ok_or_else(|| EthApiError::HeaderNotFound(target_block))?
                    .into();
            }

            let ((evm_env, _), block) = futures::try_join!(
                self.evm_env_at(target_block),
                self.recovered_block(target_block)
            )?;

            let block = block.ok_or(EthApiError::HeaderNotFound(target_block))?;

            // we're essentially replaying the transactions in the block here, hence we need the
            // state that points to the beginning of the block, which is the state at
            // the parent block
            let mut at = block.parent_hash();
            let mut replay_block_txs = true;

            let num_txs =
                transaction_index.index().unwrap_or_else(|| block.body().transactions().len());
            // but if all transactions are to be replayed, we can use the state at the block itself,
            // however only if we're not targeting the pending block, because for pending we can't
            // rely on the block's state being available
            if !is_block_target_pending && num_txs == block.body().transactions().len() {
                at = block.hash();
                replay_block_txs = false;
            }

            self.spawn_with_state_at_block(at, move |this, mut db| {
                let mut all_results = Vec::with_capacity(bundles.len());

                if replay_block_txs {
                    // only need to replay the transactions in the block if not all transactions are
                    // to be replayed
                    let block_transactions = block.transactions_recovered().take(num_txs);
                    for tx in block_transactions {
                        let tx_env = RpcNodeCore::evm_config(&this).tx_env(tx);
                        let res = this.transact(&mut db, evm_env.clone(), tx_env)?;
                        db.commit(res.state);
                    }
                }

                // transact all bundles
                for (bundle_index, bundle) in bundles.into_iter().enumerate() {
                    let Bundle { transactions, block_override } = bundle;
                    if transactions.is_empty() {
                        // Skip empty bundles
                        continue;
                    }

                    let mut bundle_results = Vec::with_capacity(transactions.len());
                    let block_overrides = block_override.map(Box::new);

                    // transact all transactions in the bundle
                    for (tx_index, tx) in transactions.into_iter().enumerate() {
                        // Apply overrides, state overrides are only applied for the first tx in the
                        // request
                        let overrides =
                            EvmOverrides::new(state_override.take(), block_overrides.clone());

                        let (current_evm_env, prepared_tx) = this
                            .prepare_call_env(evm_env.clone(), tx, &mut db, overrides)
                            .map_err(|err| {
                                Self::Error::from_eth_err(EthApiError::call_many_error(
                                    bundle_index,
                                    tx_index,
                                    err.into(),
                                ))
                            })?;
                        let res = this.transact(&mut db, current_evm_env, prepared_tx).map_err(
                            |err| {
                                Self::Error::from_eth_err(EthApiError::call_many_error(
                                    bundle_index,
                                    tx_index,
                                    err.into(),
                                ))
                            },
                        )?;

                        match Self::Error::ensure_success(res.result) {
                            Ok(output) => {
                                bundle_results
                                    .push(EthCallResponse { value: Some(output), error: None });
                            }
                            Err(err) => {
                                bundle_results.push(EthCallResponse {
                                    value: None,
                                    error: Some(err.to_string()),
                                });
                            }
                        }

                        // Commit state changes after each transaction to allow subsequent calls to
                        // see the updates
                        db.commit(res.state);
                    }

                    all_results.push(bundle_results);
                }

                Ok(all_results)
            })
            .await
        }
    }


fn call_many_parallel(
    &self,
    bundles: Vec<Bundle<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>>,
    state_context: Option<StateContext>,
    mut state_override: Option<StateOverride>,
    limit: usize,
) -> impl Future<Output = Result<Vec<Vec<EthCallResponse>>, Self::Error>> + Send {
    let this = self.clone();

    async move {
        if bundles.is_empty() {
            return Err(EthApiError::InvalidParams("bundles are empty.".into()).into());
        }

        let bundles_len = bundles.len();

        let StateContext { transaction_index, block_number } =
            state_context.unwrap_or_default();
        let transaction_index = transaction_index.unwrap_or_default();

        let mut target_block = block_number.unwrap_or_default();
        let is_block_target_pending = target_block.is_pending();

        if !is_block_target_pending {
            target_block = this
                .provider()
                .block_hash_for_id(target_block)
                .map_err(|_| EthApiError::HeaderNotFound(target_block))?
                .ok_or_else(|| EthApiError::HeaderNotFound(target_block))?
                .into();
        }

        let ((evm_env, _), block) = futures::try_join!(
            this.evm_env_at(target_block),
            this.recovered_block(target_block)
        )?;

        let block = block.ok_or(EthApiError::HeaderNotFound(target_block))?;

        let mut at = block.parent_hash();
        let mut replay_block_txs = true;

        let num_txs =
            transaction_index.index().unwrap_or_else(|| block.body().transactions().len());

        if !is_block_target_pending && num_txs == block.body().transactions().len() {
            at = block.hash();
            replay_block_txs = false;
        }

        let this2 = this.clone();

        // this2.spawn_with_state_overrides_at_block(at, state_override.take(), async move |this1, db| {
        // // let db = AsyncCacheDB::new(db);
        // // let db = CacheDBParking::new(db);
        // let db = CacheDBRwLock::new(db);


        // let concurrency = Arc::new(Semaphore::new(limit));
        // let mut join_set = JoinSet::new();
        // let total_time = Arc::new(AtomicU64::new(0));
        // for bundle in bundles.into_iter() {
        //     let permit = concurrency.clone().acquire_owned().await.unwrap();

        //     let this = this.clone();
        //     let evm_env = evm_env.clone();
        //     let block = block.clone();
        //     let mut state_override = state_override.clone();
        //     let total_time = total_time.clone();
        //     let db = db.clone();

        //     join_set.spawn(async move {
        //         let start = Instant::now();
        //         let _permit = permit;

        //             let mut db = State::builder().with_database_ref(db).build();

        //             if replay_block_txs {
        //                 let block_transactions =
        //                     block.transactions_recovered().take(num_txs);

        //                 for tx in block_transactions {
        //                     let tx_env = RpcNodeCore::evm_config(&this).tx_env(tx);
        //                     let res = this.transact(&mut db, evm_env.clone(), tx_env)?;
        //                     db.commit(res.state);
        //                 }
        //             }

        //             let Bundle { transactions, block_override } = bundle;
        //             let mut bundle_results = Vec::with_capacity(transactions.len());
        //             let block_overrides = block_override.map(Box::new);

        //             for (tx_index, tx) in transactions.into_iter().enumerate() {
        //                 let overrides =
        //                     EvmOverrides::new(state_override.take(), block_overrides.clone());

        //                 let (current_evm_env, prepared_tx) =
        //                     this.prepare_call_env(evm_env.clone(), tx, &mut db, overrides)
        //                         .map_err(|err| {
        //                             Self::Error::from_eth_err(
        //                                 EthApiError::call_many_error(0, tx_index, err.into()),
        //                             )
        //                         })?;

        //                 let res =
        //                     this.transact(&mut db, current_evm_env, prepared_tx)
        //                         .map_err(|err| {
        //                             Self::Error::from_eth_err(
        //                                 EthApiError::call_many_error(0, tx_index, err.into()),
        //                             )
        //                         })?;

        //                 match Self::Error::ensure_success(res.result) {
        //                     Ok(output) => bundle_results.push(EthCallResponse {
        //                         value: Some(output),
        //                         error: None,
        //                     }),
        //                     Err(err) => bundle_results.push(EthCallResponse {
        //                         value: None,
        //                         error: Some(err.to_string()),
        //                     }),
        //                 }

        //                 db.commit(res.state);
        //             }

        //             let elapsed = start.elapsed().as_millis() as u64;
        //             total_time.fetch_add(elapsed, Ordering::Relaxed);

        //             Ok::<Vec<EthCallResponse>, Self::Error>(bundle_results)
                
        //     });
        // }

        this2.spawn_with_state_overrides_at_block(at, state_override.take(), async move |this1, db| {

            let cache = GLOBAL_CACHE.clone();
            let mut cachedb = CacheDBRwLock::new(db);
            cachedb.cache = cache;


            let execute = move |bundles: Vec<Bundle<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>>,
                        cachedb: CacheDBRwLock<_>,
                        i: usize| {

                let this = this1.clone();
                let evm_env = evm_env.clone();
                let block = block.clone();
                let state_override_outer = state_override.clone();
                let total_time = Arc::new(AtomicU64::new(0));

                let start = Instant::now();

                let results: Result<Vec<Vec<EthCallResponse>>, Self::Error> =
                    bundles
                        .into_par_iter()
                        .map(|bundle| {

                            let start = Instant::now();

                            let this = this.clone();
                            let evm_env = evm_env.clone();
                            let block = block.clone();
                            let mut state_override = state_override_outer.clone();

                            // IMPORTANT: clone cachedb per worker
                            let db_ref = cachedb.clone();

                            let mut db = State::builder()
                                .with_database_ref(db_ref)
                                .build();

                            // replay block txs
                            if replay_block_txs {
                                let block_transactions =
                                    block.transactions_recovered().take(num_txs);

                                for tx in block_transactions {
                                    let tx_env = RpcNodeCore::evm_config(&this).tx_env(tx);
                                    let res = this.transact(&mut db, evm_env.clone(), tx_env)?;
                                    db.commit(res.state);
                                }
                            }

                            let Bundle { transactions, block_override } = bundle;

                            let mut bundle_results =
                                Vec::with_capacity(transactions.len());

                            let block_overrides = block_override.map(Box::new);

                            for (tx_index, tx) in transactions.into_iter().enumerate() {

                                let overrides = EvmOverrides::new(
                                    state_override.take(),
                                    block_overrides.clone()
                                );

                                let (current_evm_env, prepared_tx) =
                                    this.prepare_call_env(
                                        evm_env.clone(),
                                        tx,
                                        &mut db,
                                        overrides
                                    )
                                    .map_err(|err| {
                                        Self::Error::from_eth_err(
                                            EthApiError::call_many_error(
                                                0,
                                                tx_index,
                                                err.into()
                                            )
                                        )
                                    })?;

                                let res =
                                    this.transact(&mut db, current_evm_env, prepared_tx)
                                    .map_err(|err| {
                                        Self::Error::from_eth_err(
                                            EthApiError::call_many_error(
                                                0,
                                                tx_index,
                                                err.into()
                                            )
                                        )
                                    })?;

                                match Self::Error::ensure_success(res.result) {
                                    Ok(output) => bundle_results.push(
                                        EthCallResponse {
                                            value: Some(output),
                                            error: None,
                                        }
                                    ),
                                    Err(err) => bundle_results.push(
                                        EthCallResponse {
                                            value: None,
                                            error: Some(err.to_string()),
                                        }
                                    ),
                                }

                                db.commit(res.state);
                            }

                            let elapsed =
                                start.elapsed().as_millis() as u64;

                            total_time.fetch_add(
                                elapsed,
                                Ordering::Relaxed
                            );

                            Ok(bundle_results)

                        })
                        .collect();

                tracing::info!(
                    "Total time for {} parallel executions blocks {}: {} ms",
                    i + 1,
                    bundles_len,
                    total_time.load(Ordering::Relaxed)
                );

                tracing::info!("total time for all {} parallel executions: {} ms", bundles_len, start.elapsed().as_millis());

                results
            };
            // let executer = execute.clone();
            // let results1 = tokio::task::spawn_blocking({
            //     let bundles = bundles.clone();
            //     let cachedb = cachedb.clone();
            //     move || executer(bundles, cachedb, 0)
            // })
            // .await
            // .map_err(|e| {
            //     Self::Error::from_eth_err(
            //         EthApiError::InternalEthError
            //     )
            // })??;

            // cachedb.print_and_reset();

            let results2 = tokio::task::spawn_blocking({
                let cachedb = cachedb.clone();
                move || execute(bundles, cachedb, 1)
            })
            .await
            .map_err(|e| {
                Self::Error::from_eth_err(
                    EthApiError::InternalEthError
                )
            })??;

            cachedb.print_and_reset();

            Ok(results2)

        }).await

        
    }
}


    /// Creates [`AccessListResult`] for the [`RpcTxReq`] at the given
    /// [`BlockId`], or latest block.
    fn create_access_list_at(
        &self,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        block_number: Option<BlockId>,
        state_override: Option<StateOverride>,
    ) -> impl Future<Output = Result<AccessListResult, Self::Error>> + Send
    where
        Self: Trace,
    {
        async move {
            let block_id = block_number.unwrap_or_default();
            let (evm_env, at) = self.evm_env_at(block_id).await?;

            self.spawn_blocking_io_fut(move |this| async move {
                this.create_access_list_with(evm_env, at, request, state_override).await
            })
            .await
        }
    }

    /// Creates [`AccessListResult`] for the [`RpcTxReq`] at the given
    /// [`BlockId`].
    fn create_access_list_with(
        &self,
        mut evm_env: EvmEnvFor<Self::Evm>,
        at: BlockId,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        state_override: Option<StateOverride>,
    ) -> impl Future<Output = Result<AccessListResult, Self::Error>> + Send
    where
        Self: Trace,
    {
        self.spawn_blocking_io_fut(move |this| async move {
            let state = this.state_at_block_id(at).await?;
            let mut db = State::builder().with_database(StateProviderDatabase::new(state)).build();

            if let Some(state_overrides) = state_override {
                apply_state_overrides(state_overrides, &mut db)
                    .map_err(Self::Error::from_eth_err)?;
            }

            let mut tx_env = this.create_txn_env(&evm_env, request.clone(), &mut db)?;

            // we want to disable this in eth_createAccessList, since this is common practice used
            // by other node impls and providers <https://github.com/foundry-rs/foundry/issues/4388>
            evm_env.cfg_env.disable_block_gas_limit = true;

            // The basefee should be ignored for eth_createAccessList
            // See:
            // <https://github.com/ethereum/go-ethereum/blob/8990c92aea01ca07801597b00c0d83d4e2d9b811/internal/ethapi/api.go#L1476-L1476>
            evm_env.cfg_env.disable_base_fee = true;

            // Disabled because eth_createAccessList is sometimes used with non-eoa senders
            evm_env.cfg_env.disable_eip3607 = true;

            if request.as_ref().gas_limit().is_none() && tx_env.gas_price() > 0 {
                let cap = this.caller_gas_allowance(&mut db, &evm_env, &tx_env)?;
                // no gas limit was provided in the request, so we need to cap the request's gas
                // limit
                tx_env.set_gas_limit(cap.min(evm_env.block_env.gas_limit()));
            }

            // can consume the list since we're not using the request anymore
            let initial = request.as_ref().access_list().cloned().unwrap_or_default();

            let mut inspector = AccessListInspector::new(initial);

            let result = this.inspect(&mut db, evm_env.clone(), tx_env.clone(), &mut inspector)?;
            let access_list = inspector.into_access_list();
            let gas_used = result.result.gas_used();
            tx_env.set_access_list(access_list.clone());
            if let Err(err) = Self::Error::ensure_success(result.result) {
                return Ok(AccessListResult {
                    access_list,
                    gas_used: U256::from(gas_used),
                    error: Some(err.to_string()),
                });
            }

            // transact again to get the exact gas used
            let result = this.transact(&mut db, evm_env, tx_env)?;
            let gas_used = result.result.gas_used();
            let error = Self::Error::ensure_success(result.result).err().map(|e| e.to_string());

            Ok(AccessListResult { access_list, gas_used: U256::from(gas_used), error })
        })
    }
}

/// Executes code on state.
pub trait Call:
    LoadState<
        RpcConvert: RpcConvert<Evm = Self::Evm>,
        Error: FromEvmError<Self::Evm>
                   + From<<Self::RpcConvert as RpcConvert>::Error>
                   + From<ProviderError>,
    > + SpawnBlocking
{
    /// Returns default gas limit to use for `eth_call` and tracing RPC methods.
    ///
    /// Data access in default trait method implementations.
    fn call_gas_limit(&self) -> u64;

    /// Returns the maximum number of blocks accepted for `eth_simulateV1`.
    fn max_simulate_blocks(&self) -> u64;

    /// Returns the maximum memory the EVM can allocate per RPC request.
    fn evm_memory_limit(&self) -> u64;

    /// Returns the max gas limit that the caller can afford given a transaction environment.
    fn caller_gas_allowance(
        &self,
        mut db: impl Database<Error: Into<EthApiError>>,
        _evm_env: &EvmEnvFor<Self::Evm>,
        tx_env: &TxEnvFor<Self::Evm>,
    ) -> Result<u64, Self::Error> {
        alloy_evm::call::caller_gas_allowance(&mut db, tx_env).map_err(Self::Error::from_eth_err)
    }

    /// Executes the closure with the state that corresponds to the given [`BlockId`].
    fn with_state_at_block<F, R>(
        &self,
        at: BlockId,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        R: Send + 'static,
        F: FnOnce(Self, StateProviderBox) -> Result<R, Self::Error> + Send + 'static,
    {
        self.spawn_blocking_io_fut(move |this| async move {
            let state = this.state_at_block_id(at).await?;
            f(this, state)
        })
    }

    /// Executes the `TxEnv` against the given [Database] without committing state
    /// changes.
    fn transact<DB>(
        &self,
        db: DB,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
    ) -> Result<ResultAndState<HaltReasonFor<Self::Evm>>, Self::Error>
    where
        DB: Database<Error = EvmDatabaseError<ProviderError>> + fmt::Debug,
    {
        let mut evm = self.evm_config().evm_with_env(db, evm_env);
        let res = evm.transact(tx_env).map_err(Self::Error::from_evm_err)?;

        Ok(res)
    }

    /// Executes the [`reth_evm::EvmEnv`] against the given [Database] without committing state
    /// changes.
    fn transact_with_inspector<DB, I>(
        &self,
        db: DB,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
        inspector: I,
    ) -> Result<ResultAndState<HaltReasonFor<Self::Evm>>, Self::Error>
    where
        DB: Database<Error = EvmDatabaseError<ProviderError>> + fmt::Debug,
        I: InspectorFor<Self::Evm, DB>,
    {
        let mut evm = self.evm_config().evm_with_env_and_inspector(db, evm_env, inspector);
        let res = evm.transact(tx_env).map_err(Self::Error::from_evm_err)?;

        Ok(res)
    }

    /// Executes the call request at the given [`BlockId`].
    ///
    /// This spawns a new task that obtains the state for the given [`BlockId`] and then transacts
    /// the call [`Self::transact`]. If the future is dropped before the (blocking) transact
    /// call is invoked, then the task is cancelled early, (for example if the request is terminated
    /// early client-side).
    fn transact_call_at(
        &self,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        at: BlockId,
        overrides: EvmOverrides,
    ) -> impl Future<Output = Result<ResultAndState<HaltReasonFor<Self::Evm>>, Self::Error>> + Send
    where
        Self: LoadPendingBlock,
    {
        async move {
            let guard = CancelOnDrop::default();
            let cancel = guard.clone();
            let this = self.clone();

            let res = self
                .spawn_with_call_at(request, at, overrides, move |db, evm_env, tx_env| {
                    if cancel.is_cancelled() {
                        // callsite dropped the guard
                        return Err(EthApiError::InternalEthError.into())
                    }
                    let cache = GLOBAL_CACHE.clone();
                    let mut db = CacheDBRwLock::new(db);
                    db.cache = cache;
                    let db = WrapDatabaseRef(db);
                    this.transact(db, evm_env, tx_env)
                })
                .await;
            drop(guard);
            res
        }
    }

    fn transact_call_many_at(
        &self,
        bundles: Vec<Vec<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>>,
        at: BlockId,
        overrides: EvmOverrides,
        limit: usize,
    ) -> impl Future<Output = Vec<Result<Vec<ResultAndState<HaltReasonFor<Self::Evm>>>, Self::Error>>> + Send
    where
        Self: LoadPendingBlock,
    {
        async move {
            let guard = CancelOnDrop::default();
            let mut results = Vec::with_capacity(bundles.len());

            let semaphore = Arc::new(Semaphore::new(limit));
            let mut join_set = JoinSet::new();


            for bundle in bundles {
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let cancel = guard.clone();
                let this = self.clone();
                let overrides = overrides.clone();

                join_set.spawn(async move {
                    // permit is held for the lifetime of the task
                    let _permit = permit;
                    let value = this.clone();
                    this.spawn_with_calls_at(bundle, at, overrides, move |mut db, evm_env, tx_env| {
                        if cancel.is_cancelled() {
                            return Err(EthApiError::InternalEthError.into());
                        }

                        let res = value.transact(&mut db, evm_env, tx_env);

                        if let Ok(res) = &res {
                            db.commit(res.state.clone());
                        }

                        res
                    })
                    .await
                });
            }

            while let Some(res) = join_set.join_next().await {
                results.push(res.unwrap());
            }

            drop(guard);
            results
        }
    }

    /// Executes the closure with the state that corresponds to the given [`BlockId`] on a new task
    fn spawn_with_state_at_block<F, R>(
        &self,
        at: impl Into<BlockId>,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        F: FnOnce(Self, StateCacheDb) -> Result<R, Self::Error> + Send + 'static,
        R: Send + 'static,
    {
        let at = at.into();
        self.spawn_blocking_io_fut(move |this| async move {
            let state = this.state_at_block_id(at).await?;
            let db = State::builder()
                .with_database(StateProviderDatabase::new(StateProviderTraitObjWrapper(state)))
                .build();
            f(this, db)
        })
    }

    fn spawn_with_state_overrides_at_block<F, Fut, R>(
        &self,
        at: impl Into<BlockId>,
        state_overrides: Option<StateOverride>,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        F: FnOnce(
                    Self,
                    CacheDB<StateProviderDatabase<StateProviderTraitObjWrapper>>,
                ) -> Fut
                + Send
                + 'static,
        Fut: Future<Output = Result<R, Self::Error>> + Send,
        R: Send + 'static,
    {
        let at = at.into();
        self.spawn_blocking_io_fut(move |this| async move {
            let state = this.state_at_block_id(at).await?;
            let mut db = CacheDB::new(StateProviderDatabase::new(StateProviderTraitObjWrapper(state)));

            if let Some(state_overrides) = state_overrides {
                apply_state_overrides(state_overrides, &mut db)
                    .map_err(EthApiError::from_state_overrides_err)?;
            }

            f(this, db).await
        })
    }

    /// Prepares the state and env for the given [`RpcTxReq`] at the given [`BlockId`] and
    /// executes the closure on a new task returning the result of the closure.
    ///
    /// This returns the configured [`reth_evm::EvmEnv`] for the given [`RpcTxReq`] at
    /// the given [`BlockId`] and with configured call settings: `prepare_call_env`.
    ///
    /// This is primarily used by `eth_call`.
    ///
    /// # Blocking behaviour
    ///
    /// This assumes executing the call is relatively more expensive on IO than CPU because it
    /// transacts a single transaction on an empty in memory database. Because `eth_call`s are
    /// usually allowed to consume a lot of gas, this also allows a lot of memory operations so
    /// we assume this is not primarily CPU bound and instead spawn the call on a regular tokio task
    /// instead, where blocking IO is less problematic.
    fn spawn_with_call_at<F, R>(
        &self,
        request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        at: BlockId,
        overrides: EvmOverrides,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        Self: LoadPendingBlock,
        F: FnOnce(
                &mut StateCacheDb,
                EvmEnvFor<Self::Evm>,
                TxEnvFor<Self::Evm>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        async move {
            let (evm_env, at) = self.evm_env_at(at).await?;
            self.spawn_with_state_at_block(at, move |this, mut db| {
                let (evm_env, tx_env) =
                    this.prepare_call_env(evm_env, request, &mut db, overrides)?;

                f(&mut db, evm_env, tx_env)
            })
            .await
        }
    }

    fn spawn_with_calls_at<F, R>(
        &self,
        requests: Vec<RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>>,
        at: BlockId,
        mut overrides: EvmOverrides,
        f: F,
    ) -> impl Future<Output = Result<Vec<R>, Self::Error>> + Send
    where
        Self: LoadPendingBlock,
        F: Fn(
                &mut StateCacheDb,
                EvmEnvFor<Self::Evm>,
                TxEnvFor<Self::Evm>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        async move {
            let (mut current_evm_env, at) = self.evm_env_at(at).await?;
            self.spawn_with_state_at_block(at, move |this, mut db| {
                let mut res = Vec::with_capacity(requests.len());

                for request in requests {
                    let overrides =
                        EvmOverrides::new(overrides.state.take(), overrides.block.take());

                    let (evm_env, tx_env) =
                        this.prepare_call_env(current_evm_env, request, &mut db, overrides)?;

                    current_evm_env = evm_env.clone();
                    res.push(f(&mut db, evm_env, tx_env)?);
                }

                Ok(res)

            })
            .await
        }
    }

    /// Retrieves the transaction if it exists and executes it.
    ///
    /// Before the transaction is executed, all previous transaction in the block are applied to the
    /// state by executing them first.
    /// The callback `f` is invoked with the [`ResultAndState`] after the transaction was executed
    /// and the database that points to the beginning of the transaction.
    ///
    /// Note: Implementers should use a threadpool where blocking is allowed, such as
    /// [`BlockingTaskPool`](reth_tasks::pool::BlockingTaskPool).
    fn spawn_replay_transaction<F, R>(
        &self,
        hash: B256,
        f: F,
    ) -> impl Future<Output = Result<Option<R>, Self::Error>> + Send
    where
        Self: LoadBlock + LoadTransaction,
        F: FnOnce(
                TransactionInfo,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        async move {
            let (transaction, block) = match self.transaction_and_block(hash).await? {
                None => return Ok(None),
                Some(res) => res,
            };
            let (tx, tx_info) = transaction.split();

            let evm_env = self.evm_env_for_header(block.sealed_block().sealed_header())?;

            // we need to get the state of the parent block because we're essentially replaying the
            // block the transaction is included in
            let parent_block = block.parent_hash();

            self.spawn_with_state_at_block(parent_block, move |this, mut db| {
                let block_txs = block.transactions_recovered();

                // replay all transactions prior to the targeted transaction
                this.replay_transactions_until(&mut db, evm_env.clone(), block_txs, *tx.tx_hash())?;

                let tx_env = RpcNodeCore::evm_config(&this).tx_env(tx);

                let res = this.transact(&mut db, evm_env, tx_env)?;
                f(tx_info, res, db)
            })
            .await
            .map(Some)
        }
    }

    /// Replays all the transactions until the target transaction is found.
    ///
    /// All transactions before the target transaction are executed and their changes are written to
    /// the _runtime_ db ([`State`]).
    ///
    /// Note: This assumes the target transaction is in the given iterator.
    /// Returns the index of the target transaction in the given iterator.
    fn replay_transactions_until<'a, DB, I>(
        &self,
        db: &mut DB,
        evm_env: EvmEnvFor<Self::Evm>,
        transactions: I,
        target_tx_hash: B256,
    ) -> Result<usize, Self::Error>
    where
        DB: Database<Error = EvmDatabaseError<ProviderError>> + DatabaseCommit + core::fmt::Debug,
        I: IntoIterator<Item = Recovered<&'a ProviderTx<Self::Provider>>>,
    {
        let mut evm = self.evm_config().evm_with_env(db, evm_env);
        let mut index = 0;
        for tx in transactions {
            if *tx.tx_hash() == target_tx_hash {
                // reached the target transaction
                break
            }

            let tx_env = self.evm_config().tx_env(tx);
            evm.transact_commit(tx_env).map_err(Self::Error::from_evm_err)?;
            index += 1;
        }
        Ok(index)
    }

    ///
    /// All `TxEnv` fields are derived from the given [`RpcTxReq`], if fields are
    /// `None`, they fall back to the [`reth_evm::EvmEnv`]'s settings.
    fn create_txn_env(
        &self,
        evm_env: &EvmEnvFor<Self::Evm>,
        mut request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        mut db: impl Database<Error: Into<EthApiError>>,
    ) -> Result<TxEnvFor<Self::Evm>, Self::Error> {
        if request.as_ref().nonce().is_none() {
            let nonce = db
                .basic(request.as_ref().from().unwrap_or_default())
                .map_err(Into::into)?
                .map(|acc| acc.nonce)
                .unwrap_or_default();
            request.as_mut().set_nonce(nonce);
        }

        Ok(self.converter().tx_env(request, evm_env)?)
    }

    /// Prepares the [`reth_evm::EvmEnv`] for execution of calls.
    ///
    /// Does not commit any changes to the underlying database.
    ///
    /// ## EVM settings
    ///
    /// This modifies certain EVM settings to mirror geth's `SkipAccountChecks` when transacting requests, see also: <https://github.com/ethereum/go-ethereum/blob/380688c636a654becc8f114438c2a5d93d2db032/core/state_transition.go#L145-L148>:
    ///
    ///  - `disable_eip3607` is set to `true`
    ///  - `disable_base_fee` is set to `true`
    ///  - `nonce` is set to `None`
    ///
    /// In addition, this changes the block's gas limit to the configured [`Self::call_gas_limit`].
    #[expect(clippy::type_complexity)]
    fn prepare_call_env<DB>(
        &self,
        mut evm_env: EvmEnvFor<Self::Evm>,
        mut request: RpcTxReq<<Self::RpcConvert as RpcConvert>::Network>,
        db: &mut DB,
        overrides: EvmOverrides,
    ) -> Result<(EvmEnvFor<Self::Evm>, TxEnvFor<Self::Evm>), Self::Error>
    where
        DB: Database + DatabaseCommit + OverrideBlockHashes,
        EthApiError: From<<DB as Database>::Error>,
    {
        // track whether the request has a gas limit set
        let request_has_gas_limit = request.as_ref().gas_limit().is_some();

        if let Some(requested_gas) = request.as_ref().gas_limit() {
            let global_gas_cap = self.call_gas_limit();
            if global_gas_cap != 0 && global_gas_cap < requested_gas {
                warn!(target: "rpc::eth::call", ?request, ?global_gas_cap, "Capping gas limit to global gas cap");
                request.as_mut().set_gas_limit(global_gas_cap);
            }
        } else {
            // cap request's gas limit to call gas limit
            request.as_mut().set_gas_limit(self.call_gas_limit());
        }

        // Disable block gas limit check to allow executing transactions with higher gas limit (call
        // gas limit): https://github.com/paradigmxyz/reth/issues/18577
        evm_env.cfg_env.disable_block_gas_limit = true;

        // Disabled because eth_call is sometimes used with eoa senders
        // See <https://github.com/paradigmxyz/reth/issues/1959>
        evm_env.cfg_env.disable_eip3607 = true;

        // The basefee should be ignored for eth_call
        // See:
        // <https://github.com/ethereum/go-ethereum/blob/ee8e83fa5f6cb261dad2ed0a7bbcde4930c41e6c/internal/ethapi/api.go#L985>
        evm_env.cfg_env.disable_base_fee = true;

        // Disable EIP-7825 transaction gas limit to support larger transactions
        evm_env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);

        // Disable additional fee charges, e.g. opstack operator fee charge
        // See:
        // <https://github.com/paradigmxyz/reth/issues/18470>
        evm_env.cfg_env.disable_fee_charge = true;

        evm_env.cfg_env.memory_limit = self.evm_memory_limit();

        // set nonce to None so that the correct nonce is chosen by the EVM
        request.as_mut().take_nonce();

        if let Some(block_overrides) = overrides.block {
            apply_block_overrides(*block_overrides, db, evm_env.block_env.inner_mut());
        }
        if let Some(state_overrides) = overrides.state {
            apply_state_overrides(state_overrides, db)
                .map_err(EthApiError::from_state_overrides_err)?;
        }

        let mut tx_env = self.create_txn_env(&evm_env, request, &mut *db)?;

        // lower the basefee to 0 to avoid breaking EVM invariants (basefee < gasprice): <https://github.com/ethereum/go-ethereum/blob/355228b011ef9a85ebc0f21e7196f892038d49f0/internal/ethapi/api.go#L700-L704>
        if tx_env.gas_price() == 0 {
            evm_env.block_env.inner_mut().basefee = 0;
        }

        if !request_has_gas_limit {
            // No gas limit was provided in the request, so we need to cap the transaction gas limit
            if tx_env.gas_price() > 0 {
                // If gas price is specified, cap transaction gas limit with caller allowance
                trace!(target: "rpc::eth::call", ?tx_env, "Applying gas limit cap with caller allowance");
                let cap = self.caller_gas_allowance(db, &evm_env, &tx_env)?;
                // ensure we cap gas_limit to the block's
                tx_env.set_gas_limit(cap.min(evm_env.block_env.gas_limit()));
            }
        }

        Ok((evm_env, tx_env))
    }
}
