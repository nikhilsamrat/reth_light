use alloy_consensus::Header;
use alloy_primitives::{hex, BlockHash};
use clap::Parser;
use reth_db::static_file::{
    ColumnSelectorOne, ColumnSelectorTwo, HeaderWithHashMask, ReceiptMask, TransactionMask,
};
use reth_db::{init_db, mdbx::DatabaseArguments, DatabaseEnv};
use reth_db_api::{
    table::{Decompress, DupSort, Table},
    tables, RawKey, RawTable, Receipts, TableViewer, Transactions,
};

use reth_provider::DatabaseProviderFactory;

use reth_db_api::{
    cursor::DbCursorRO, cursor::DbDupCursorRO, database::Database, models::ClientVersion, table::TableImporter,
    transaction::DbTx,
};

use reth_chainspec::{EthChainSpec, EthereumHardforks};


use reth_db_common::DbTool;
use reth_node_api::{ReceiptTy, TxTy};
use reth_node_builder::NodeTypesWithDB;
use reth_provider::{providers::ProviderNodeTypes, StaticFileProviderFactory};
use reth_static_file_types::StaticFileSegment;
use tracing::{error, info};

use reth_node_core::{
    args::DatadirArgs,
    dirs::{DataDirPath, PlatformPath},
};

use std::{path::PathBuf, sync::Arc};


/// The arguments for the `reth db get` command
#[derive(Parser, Debug)]
pub struct Command {
    /// The path to the new datadir folder.
    #[arg(long, value_name = "OUTPUT_PATH", verbatim_doc_comment)]
    output_datadir: PlatformPath<DataDirPath>,
    table: tables::Tables,
}

impl Command {
    /// Execute `db get` command
    pub fn execute<N: ProviderNodeTypes>(self, tool: &DbTool<N>) -> eyre::Result<()> {
        let output_datadir =
            self.output_datadir.with_chain(tool.chain().chain(), DatadirArgs::default());
        // let output_db = setup(&output_datadir.db())?;

        // match self.table {
        //     tables::Tables::AccountsTrie | tables::Tables::StoragesTrie => {

        //     }
        //     tables::Tables::StageCheckpoints | tables::Tables::StageCheckpointProgresses => {
        //         self.table.view(&GetValueViewer { tool, output_db })?;
        //     }
        //     tables::Tables::PruneCheckpoints => {
        //         self.table.view(&GetValueViewer { tool, output_db })?;
        //     }
        //     tables::Tables::BlockBodyIndices |
        //     tables::Tables::BlockOmmers |
        //     tables::Tables::ChainState |
        //     tables::Tables::TransactionBlocks |
        //     tables::Tables::TransactionSenders |
        //     tables::Tables::VersionHistory |
        //     tables::Tables::AccountChangeSets
        //     => {
        //         self.table.view(&GetValueViewer { tool, output_db })?;
        //     }
        //     tables::Tables::PlainAccountState |
        //     tables::Tables::PlainStorageState |
        //     tables::Tables::BlockWithdrawals |
        //     tables::Tables::Bytecodes |
        //     tables::Tables::HeaderNumbers 
        //      => {
        //         self.table.view(&GetLargeValueViewer { tool, output_db })?;
        //     }
        //     _ => {

        //     }
        // }

        // // StageCheckpoints and StageCheckpointProgresses
        // tables::Tables::StageCheckpoints.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::StageCheckpointProgresses.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;

        // // PruneCheckpoints
        // tables::Tables::PruneCheckpoints.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;

        // // Tables using GetValueViewer
        // tables::Tables::BlockBodyIndices.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::BlockOmmers.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::ChainState.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::TransactionBlocks.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::TransactionSenders.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::VersionHistory.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        // tables::Tables::AccountChangeSets.view(&GetValueViewer { tool, output_db: setup(&output_datadir.db())? })?;

        // Tables using GetLargeValueViewer
        tables::Tables::PlainAccountState.view(&GetLargeValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        tables::Tables::PlainStorageState.view(&GetLargeValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        tables::Tables::BlockWithdrawals.view(&GetLargeValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        tables::Tables::Bytecodes.view(&GetLargeValueViewer { tool, output_db: setup(&output_datadir.db())? })?;
        tables::Tables::HeaderNumbers.view(&GetLargeValueViewer { tool, output_db: setup(&output_datadir.db())? })?;

        Ok(())
    }
}


struct GetValueViewer<'a, N: NodeTypesWithDB> {
    tool: &'a DbTool<N>,
    output_db: DatabaseEnv,
}

impl<N: ProviderNodeTypes> TableViewer<()> for GetValueViewer<'_, N> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> Result<(), Self::Error> {
        let provider = self.tool.provider_factory.database_provider_ro()?;
        let inner_tx = provider.into_tx();
        self.output_db.update(|tx| tx.import_table::<T, _>(&inner_tx))??;
        inner_tx.commit()?;
        Ok(())
    }

    fn view_dupsort<T: DupSort>(&self) -> Result<(), Self::Error> {
        let provider = self.tool.provider_factory.database_provider_ro()?;
        let inner_tx = provider.into_tx();
        self.output_db.update(|tx| tx.import_dupsort::<T, _>(&inner_tx))??;
        inner_tx.commit()?;
        Ok(())
    }
}

struct GetLargeValueViewer<'a, N: NodeTypesWithDB> {
    tool: &'a DbTool<N>,
    output_db: DatabaseEnv,
}

impl<N: ProviderNodeTypes> TableViewer<()> for GetLargeValueViewer<'_, N> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> Result<(), Self::Error> {
        let limit = 1_000_00;
        let provider = self.tool.provider_factory.database_provider_ro()?;
        let inner_tx = provider.into_tx();
        let last = self.output_db.view(|tx| tx.cursor_read::<T>().unwrap().last())??.map(|kv| kv.0);
        let mut from = if let Some(last) = last {
            let mut cursor = inner_tx.cursor_read::<T>()?;
            cursor.seek(last)?;
            let from = cursor.next()?;
            if from.is_none() {
                return Ok(());
            }
            from.map(|kv| kv.0)
        } else {
            None
        };
        let to = inner_tx.cursor_read::<T>()?.last()?.map(|kv| kv.0);
        inner_tx.commit()?;
        info!("Importing table from {:?} to {:?}", from, to);
        if let Some(to) = to {
            loop {
                let provider = self.tool.provider_factory.database_provider_ro()?;
                let inner_tx = provider.into_tx();
                from = self.output_db.update(|tx| tx.import_table_with_range_limit::<T, _>(&inner_tx, from.clone(), limit, to.clone()))??;
                inner_tx.commit()?;
                if from.is_none() {
                    break;
                }
                info!("Imported {} entries", limit);
            }
        }   
        Ok(())
    }

    fn view_dupsort<T: DupSort>(&self) -> Result<(), Self::Error> {
        let limit = 1_000_000;
        let provider = self.tool.provider_factory.database_provider_ro()?;
        let inner_tx = provider.into_tx();
        let mut last_key = self.output_db.view(|tx| tx.cursor_read::<T>().unwrap().last())??.map(|kv| kv.0);
        let mut last_subkey = if let Some(last_key) = last_key.clone() {
            self.output_db.view(|tx| {
                let mut cursor = tx.cursor_read::<T>().unwrap();
                cursor.seek(last_key).unwrap();
                let mut value = None;
                while let Some(v) = cursor.next_dup_val().unwrap() {
                    value = T::get_subkey(&v);
                }
                value
            })?
        } else {
            None
        };
        inner_tx.commit()?;
        info!("Importing table from {:?} to {:?}", last_key, last_subkey);
        loop {
            let provider = self.tool.provider_factory.database_provider_ro()?;
            let inner_tx = provider.into_tx();
            let res = self.output_db.update(|tx| tx.import_dupsort_limit::<T, _>(&inner_tx, last_key, last_subkey, limit))??;
            inner_tx.commit()?;
            if res.is_none() {
                break;
            }
            let (k, sk) = res.unwrap();
            last_key = Some(k);
            last_subkey = Some(sk);
            info!("Imported {} entries", limit);
        }
        Ok(())
    }
}

/// Sets up the database and initial state on [`tables::BlockBodyIndices`]. Also returns the tip
/// block number.
fn setup(
    output_db: &PathBuf,
) -> eyre::Result<DatabaseEnv> {

    let output_datadir = init_db(output_db, DatabaseArguments::new(ClientVersion::default()))?;

    Ok(output_datadir)
}


#[cfg(test)]
mod tests {
    // use super::*;
    // use alloy_primitives::{address, B256};
    // use clap::{Args, Parser};
    // use reth_db_api::{
    //     models::{storage_sharded_key::StorageShardedKey, ShardedKey},
    //     AccountsHistory, HashedAccounts, Headers, StageCheckpoints, StoragesHistory,
    // };
    // use std::str::FromStr;

    // /// A helper type to parse Args more easily
    // #[derive(Parser)]
    // struct CommandParser<T: Args> {
    //     #[command(flatten)]
    //     args: T,
    // }

    // #[test]
    // fn parse_numeric_key_args() {
    //     assert_eq!(table_key::<Headers>("123").unwrap(), 123);
    //     assert_eq!(
    //         table_key::<HashedAccounts>(
    //             "\"0x0ac361fe774b78f8fc4e86c1916930d150865c3fc2e21dca2e58833557608bac\""
    //         )
    //         .unwrap(),
    //         B256::from_str("0x0ac361fe774b78f8fc4e86c1916930d150865c3fc2e21dca2e58833557608bac")
    //             .unwrap()
    //     );
    // }

    // #[test]
    // fn parse_string_key_args() {
    //     assert_eq!(
    //         table_key::<StageCheckpoints>("\"MerkleExecution\"").unwrap(),
    //         "MerkleExecution"
    //     );
    // }

    // #[test]
    // fn parse_json_key_args() {
    //     assert_eq!(
    //         table_key::<StoragesHistory>(r#"{ "address": "0x01957911244e546ce519fbac6f798958fafadb41", "sharded_key": { "key": "0x0000000000000000000000000000000000000000000000000000000000000003", "highest_block_number": 18446744073709551615 } }"#).unwrap(),
    //         StorageShardedKey::new(
    //             address!("0x01957911244e546ce519fbac6f798958fafadb41"),
    //             B256::from_str(
    //                 "0x0000000000000000000000000000000000000000000000000000000000000003"
    //             )
    //             .unwrap(),
    //             18446744073709551615
    //         )
    //     );
    // }

    // #[test]
    // fn parse_json_key_for_account_history() {
    //     assert_eq!(
    //         table_key::<AccountsHistory>(r#"{ "key": "0x4448e1273fd5a8bfdb9ed111e96889c960eee145", "highest_block_number": 18446744073709551615 }"#).unwrap(),
    //         ShardedKey::new(
    //             address!("0x4448e1273fd5a8bfdb9ed111e96889c960eee145"),
    //             18446744073709551615
    //         )
    //     );
    // }
}
