use crate::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};


use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait that will transform the data to be saved in the DB in a (ideally) compressed format
pub trait Compress: Send + Sync + Sized + Debug {
    /// Compressed type.
    type Compressed: bytes::BufMut
        + AsRef<[u8]>
        + AsMut<[u8]>
        + Into<Vec<u8>>
        + Default
        + Send
        + Sync
        + Debug;

    /// If the type cannot be compressed, return its inner reference as `Some(self.as_ref())`
    fn uncompressable_ref(&self) -> Option<&[u8]> {
        None
    }

    /// Compresses data going into the database.
    fn compress(self) -> Self::Compressed {
        let mut buf = Self::Compressed::default();
        self.compress_to_buf(&mut buf);
        buf
    }

    /// Compresses data to a given buffer.
    fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B);
}

/// Trait that will transform the data to be read from the DB.
pub trait Decompress: Send + Sync + Sized + Debug {
    /// Decompresses data coming from the database.
    fn decompress(value: &[u8]) -> Result<Self, DatabaseError>;

    /// Decompresses owned data coming from the database.
    fn decompress_owned(value: Vec<u8>) -> Result<Self, DatabaseError> {
        Self::decompress(&value)
    }
}

/// Trait that will transform the data to be saved in the DB.
pub trait Encode: Send + Sync + Sized + Debug {
    /// Encoded type.
    type Encoded: AsRef<[u8]> + Into<Vec<u8>> + Send + Sync + Ord + Debug;

    /// Encodes data going into the database.
    fn encode(self) -> Self::Encoded;
}

/// Trait that will transform the data to be read from the DB.
pub trait Decode: Send + Sync + Sized + Debug {
    /// Decodes data coming from the database.
    fn decode(value: &[u8]) -> Result<Self, DatabaseError>;

    /// Decodes owned data coming from the database.
    fn decode_owned(value: Vec<u8>) -> Result<Self, DatabaseError> {
        Self::decode(&value)
    }
}

/// Generic trait that enforces the database key to implement [`Encode`] and [`Decode`].
pub trait Key: Encode + Decode + Ord + Clone + Serialize + for<'a> Deserialize<'a> {}

impl<T> Key for T where T: Encode + Decode + Ord + Clone + Serialize + for<'a> Deserialize<'a> {}

/// Generic trait that enforces the database value to implement [`Compress`] and [`Decompress`].
pub trait ValueInner: Compress + Decompress + Serialize {}

/// Generic trait that enforces the database value to implement [`Compress`] and [`Decompress`].
pub trait Value: ValueInner + for<'a> Deserialize<'a>{}

impl<T> ValueInner for T where T: Compress + Decompress + Serialize {}

impl<T> Value for T where T: ValueInner + for<'a> Deserialize<'a>{}



/// Generic trait that a database table should follow.
///
/// The [`Table::Key`] and [`Table::Value`] types should implement [`Encode`] and
/// [`Decode`] when appropriate. These traits define how the data is stored and read from the
/// database.
///
/// It allows for the use of codecs. See [`crate::models::ShardedKey`] for a custom
/// implementation.
pub trait Table: Send + Sync + Debug + 'static {
    /// The table's name.
    const NAME: &'static str;

    /// Whether the table is also a `DUPSORT` table.
    const DUPSORT: bool;

    /// Key element of `Table`.
    ///
    /// Sorting should be taken into account when encoding this.
    type Key: Key;

    /// Value element of `Table`.
    type Value: Value;
}

/// Trait that provides object-safe access to the table's metadata.
pub trait TableInfo: Send + Sync + Debug + 'static {
    /// The table's name.
    fn name(&self) -> &'static str;

    /// Whether the table is a `DUPSORT` table.
    fn is_dupsort(&self) -> bool;
}

/// Tuple with `T::Key` and `T::Value`.
pub type TableRow<T> = (<T as Table>::Key, <T as Table>::Value);

/// `DupSort` allows for keys to be repeated in the database.
///
/// Upstream docs: <https://libmdbx.dqdkfa.ru/usage.html#autotoc_md48>
pub trait DupSort: Table {
    /// The table subkey. This type must implement [`Encode`] and [`Decode`].
    ///
    /// Sorting should be taken into account when encoding this.
    ///
    /// Upstream docs: <https://libmdbx.dqdkfa.ru/usage.html#autotoc_md48>
    type SubKey: Key;
    fn get_subkey(value: &Self::Value) -> Option<Self::SubKey> {
        None
    }
}

/// Allows duplicating tables across databases
pub trait TableImporter: DbTxMut {
    /// Imports all table data from another transaction.
    fn import_table<T: Table, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_write::<T>()?;

        for kv in source_tx.cursor_read::<T>()?.walk(None)? {
            let (k, v) = kv?;
            destination_cursor.append(k, &v)?;
        }

        Ok(())
    }

    /// Imports table data from another transaction within a range.
    fn import_table_with_range<T: Table, R: DbTx>(
        &self,
        source_tx: &R,
        from: Option<<T as Table>::Key>,
        to: <T as Table>::Key,
    ) -> Result<(), DatabaseError>
    where
        T::Key: Default,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let source_range = match from {
            Some(from) => source_cursor.walk_range(from..=to),
            None => source_cursor.walk_range(..=to),
        };
        for row in source_range? {
            let (key, value) = row?;
            destination_cursor.append(key, &value)?;
        }

        Ok(())
    }

    /// Imports table data from another transaction within a range.
    fn import_table_with_range_limit<T: Table, R: DbTx>(
        &self,
        source_tx: &R,
        from: Option<<T as Table>::Key>,
        limit: usize,
        to: <T as Table>::Key,
    ) -> Result<Option<<T as Table>::Key>, DatabaseError>
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let source_range = match from {
            Some(from) => source_cursor.walk_range(from..=to),
            None => source_cursor.walk_range(..=to),
        };
        let mut count = 0;
        for row in source_range? {
            let (key, value) = row?;
            if count >= limit {
                return Ok(Some(key));
            }
            destination_cursor.append(key, &value)?;
            count += 1;
        }

        Ok(None)
    }

    /// Imports all dupsort data from another transaction.
    fn import_dupsort<T: DupSort, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut cursor = source_tx.cursor_dup_read::<T>()?;

        while let Some((k, _)) = cursor.next_no_dup()? {
            for kv in cursor.walk_dup(Some(k), None)? {
                let (k, v) = kv?;
                destination_cursor.append_dup(k, v)?;
            }
        }

        Ok(())
    }

    /// Imports table data from another transaction within a range.
    fn import_dupsort_limit<T: DupSort, R: DbTx>(
        &self,
        source_tx: &R,
        from_key: Option<<T as Table>::Key>,
        from_subkey: Option<<T as DupSort>::SubKey>,
        limit: usize,
    ) -> Result<Option<(<T as Table>::Key, <T as DupSort>::SubKey)>, DatabaseError>
    {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut source_cursor = source_tx.cursor_dup_read::<T>()?;

        let mut count = 0;

        if let Some(from_subkey) = from_subkey {
            let from_key = from_key.unwrap_or_else(||{
                destination_cursor.first().unwrap().unwrap().0
            });
            source_cursor.seek_by_key_subkey(from_key.clone(), from_subkey)?;

            while let Some(v) = source_cursor.next_dup_val()? {
                let sub_key = T::get_subkey(&v).unwrap();
                destination_cursor.append_dup(from_key.clone(), v)?;
                count += 1;
                if count >= limit {
                    return Ok(Some((from_key, sub_key)));
                }
            }
        }

        while let Some((k, _)) = source_cursor.next_no_dup()? {
            for kv in source_cursor.walk_dup(Some(k), None)? {
                let (k, v) = kv?;
                let sub_key = T::get_subkey(&v).unwrap();
                destination_cursor.append_dup(k.clone(), v)?;
                count += 1;
                if count >= limit {
                    return Ok(Some((k, sub_key)));
                }
            }
        }

        Ok(None)
    }
}


