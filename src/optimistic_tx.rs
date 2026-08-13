use std::iter::Rev;
use std::ops::RangeBounds;

use anyhow::Result;
use fjall::{
    Guard, Iter, Keyspace, OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Slice, Snapshot, UserKey, UserValue,
};

use crate::ReturnType;

pub trait Readable {
    fn get<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<Option<UserValue>>;
    fn contains_key<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<bool>;
    fn iter(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Iter;
    fn prefix<K: AsRef<[u8]>>(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace, prefix: K) -> Iter;
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        range: R,
    ) -> Iter;
    fn is_empty(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<bool>;
    fn len(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<usize>;
    fn size_of<K: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        key: K,
    ) -> fjall::Result<Option<u32>>;
    fn first_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard>;
    fn last_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard>;
}

pub struct NoTx;

impl Readable for NoTx {
    fn get<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<Option<UserValue>> {
        Ok(partition.get(key)?)
    }
    fn contains_key<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<bool> {
        Ok(partition.contains_key(key)?)
    }
    fn iter(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Iter {
        fjall::Readable::iter(&database.read_tx(), partition)
    }
    fn prefix<K: AsRef<[u8]>>(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace, prefix: K) -> Iter {
        fjall::Readable::prefix(&database.read_tx(), partition, prefix)
    }
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        range: R,
    ) -> Iter {
        fjall::Readable::range(&database.read_tx(), partition, range)
    }
    fn is_empty(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<bool> {
        Ok(fjall::Readable::is_empty(&database.read_tx(), partition)?)
    }
    fn len(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<usize> {
        Ok(fjall::Readable::len(&database.read_tx(), partition)?)
    }
    fn size_of<K: AsRef<[u8]>>(
        &self,
        _database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        key: K,
    ) -> fjall::Result<Option<u32>> {
        partition.size_of(key)
    }
    fn first_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        partition.first_key_value()
    }
    fn last_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        partition.last_key_value()
    }
}

impl Readable for OptimisticWriteTx {
    fn get<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<Option<UserValue>> {
        fjall::Readable::get(self, partition, key)
    }
    fn contains_key<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<bool> {
        fjall::Readable::contains_key(self, partition, key)
    }
    fn iter(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Iter {
        fjall::Readable::iter(self, partition)
    }
    fn prefix<K: AsRef<[u8]>>(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace, prefix: K) -> Iter {
        fjall::Readable::prefix(self, partition, prefix)
    }
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        _: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        range: R,
    ) -> Iter {
        fjall::Readable::range(self, partition, range)
    }
    fn is_empty(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<bool> {
        Ok(fjall::Readable::is_empty(self, partition)?)
    }
    fn len(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<usize> {
        Ok(fjall::Readable::len(self, partition)?)
    }
    fn size_of<K: AsRef<[u8]>>(
        &self,
        _: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        key: K,
    ) -> fjall::Result<Option<u32>> {
        Ok(fjall::Readable::size_of(self, partition, key)?)
    }
    fn first_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        fjall::Readable::first_key_value(self, partition)
    }
    fn last_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        fjall::Readable::last_key_value(self, partition)
    }
}

impl Readable for Snapshot {
    fn get<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<Option<UserValue>> {
        Ok(fjall::Readable::get(self, partition, key)?)
    }
    fn contains_key<K: AsRef<[u8]>>(&self, partition: &OptimisticTxKeyspace, key: K) -> fjall::Result<bool> {
        Ok(fjall::Readable::contains_key(self, partition, key)?)
    }
    fn iter(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Iter {
        fjall::Readable::iter(self, partition)
    }
    fn prefix<K: AsRef<[u8]>>(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace, prefix: K) -> Iter {
        fjall::Readable::prefix(self, partition, prefix)
    }
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        _: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        range: R,
    ) -> Iter {
        fjall::Readable::range(self, partition, range)
    }
    fn is_empty(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<bool> {
        fjall::Readable::is_empty(self, partition)
    }
    fn len(&self, _: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Result<usize> {
        fjall::Readable::len(self, partition)
    }
    fn size_of<K: AsRef<[u8]>>(
        &self,
        _: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        key: K,
    ) -> fjall::Result<Option<u32>> {
        fjall::Readable::size_of(self, partition, key)
    }
    fn first_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        fjall::Readable::first_key_value(self, partition)
    }
    fn last_key_value(&self, partition: &OptimisticTxKeyspace) -> Option<Guard> {
        fjall::Readable::last_key_value(self, partition)
    }
}

pub trait IterExtensions {
    fn iter_guarded(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Iter;

    fn iter_keys(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserKey>>;

    fn iter_values(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserValue>>;

    fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<(UserKey, UserValue)>>;

    fn iter_guarded_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> fjall::Iter;

    fn iter_keys_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserKey>>;

    fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserValue>>;

    fn iter_guarded_rev(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Rev<fjall::Iter>;

    fn iter_keys_rev(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserKey>>;

    fn iter_values_rev(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserValue>>;

    fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<(UserKey, UserValue)>>;

    fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> Rev<fjall::Iter>;

    fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserKey>>;

    fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserValue>>;
}

impl<T: Readable> IterExtensions for T {
    fn iter_guarded(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> fjall::Iter {
        self.iter(database, partition)
    }

    fn iter_keys(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserKey>> {
        self.iter(database, partition).map(|guard| Ok(guard.key()?))
    }

    fn iter_values(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserValue>> {
        self.iter(database, partition).map(|guard| Ok(guard.value()?))
    }

    fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<(UserKey, UserValue)>> {
        self.prefix(database, partition, prefix).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key, value))
        })
    }

    fn iter_guarded_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> fjall::Iter {
        self.prefix(database, partition, prefix)
    }

    fn iter_keys_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserKey>> {
        self.prefix(database, partition, prefix).map(|guard| Ok(guard.key()?))
    }

    fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserValue>> {
        self.prefix(database, partition, prefix).map(|guard| Ok(guard.value()?))
    }

    fn iter_guarded_rev(&self, database: &OptimisticTxDatabase, partition: &OptimisticTxKeyspace) -> Rev<fjall::Iter> {
        self.iter(database, partition).rev()
    }

    fn iter_keys_rev(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserKey>> {
        self.iter(database, partition).rev().map(|guard| Ok(guard.key()?))
    }

    fn iter_values_rev(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
    ) -> impl Iterator<Item = Result<UserValue>> {
        self.iter(database, partition).rev().map(|guard| Ok(guard.value()?))
    }

    fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<(UserKey, UserValue)>> {
        self.prefix(database, partition, prefix).rev().map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key, value))
        })
    }

    fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> Rev<fjall::Iter> {
        self.prefix(database, partition, prefix).rev()
    }

    fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserKey>> {
        self.prefix(database, partition, prefix).rev().map(|guard| Ok(guard.key()?))
    }

    fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        database: &OptimisticTxDatabase,
        partition: &OptimisticTxKeyspace,
        prefix: P,
    ) -> impl Iterator<Item = Result<UserValue>> {
        self.prefix(database, partition, prefix).rev().map(|guard| Ok(guard.value()?))
    }
}

pub fn tx_update<K: Into<UserKey>, F: FnOnce(Option<&UserValue>) -> Option<UserValue>>(
    tx: &mut OptimisticWriteTx,
    keyspace: impl AsRef<Keyspace>,
    key: K,
    f: F,
    return_type: ReturnType,
) -> fjall::Result<Option<UserValue>> {
    match return_type {
        ReturnType::NewValue => tx.update_fetch(keyspace, key, f),
        ReturnType::OldValue => tx.fetch_update(keyspace, key, f),
    }
}

pub trait WriteTxExtensions {
    fn update<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>;

    fn update_with<K: Into<UserKey>, V: Into<UserValue>, F>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Option<V>;

    fn try_update_with<K: Into<UserKey>, V: Into<UserValue>, F>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Result<Option<V>>;

    fn update_or_insert<K: Into<UserKey>, F, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Option<V>;

    fn update_or_insert_with<K: Into<UserKey>, F1, F2, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F1: FnOnce(&UserValue) -> Option<V>,
        F2: FnOnce() -> Option<V>;

    fn try_update_or_insert<K: Into<UserKey>, F, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Result<Option<V>>;

    fn try_update_or_insert_with<K: Into<UserKey>, F1, F2, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F1: FnOnce(&UserValue) -> Result<Option<V>>,
        F2: FnOnce() -> Result<Option<V>>;
}

impl WriteTxExtensions for OptimisticWriteTx {
    fn update<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>> {
        let mut value = value.map(|v| v.into());
        Ok(tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(_) => value.take(),
                None => None,
            },
            return_type,
        )?)
    }

    fn update_with<K: Into<UserKey>, V: Into<UserValue>, F>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Option<V>,
    {
        Ok(tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => update(old_value).map(|v| v.into()),
                None => None,
            },
            return_type,
        )?)
    }

    fn try_update_with<K: Into<UserKey>, V: Into<UserValue>, F>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Result<Option<V>>,
    {
        let mut error = None;

        let result = tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => match update(old_value) {
                    Ok(new_value) => new_value.map(|v| v.into()),
                    Err(err) => {
                        error = Some(err);
                        Some(old_value.clone())
                    }
                },
                None => None,
            },
            return_type,
        )?;

        match error {
            Some(err) => Err(err),
            None => Ok(result),
        }
    }

    fn update_or_insert<K: Into<UserKey>, F, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Option<V>,
    {
        let mut value = value.map(|v| v.into());
        Ok(tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => update(old_value).map(|v| v.into()),
                None => value.take(),
            },
            return_type,
        )?)
    }

    fn update_or_insert_with<K: Into<UserKey>, F1, F2, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F1: FnOnce(&UserValue) -> Option<V>,
        F2: FnOnce() -> Option<V>,
    {
        Ok(tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => update(old_value).map(|v| v.into()),
                None => insert().map(|v| v.into()),
            },
            return_type,
        )?)
    }

    fn try_update_or_insert<K: Into<UserKey>, F, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F,
        value: Option<V>,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F: FnOnce(&UserValue) -> Result<Option<V>>,
    {
        let mut value = value.map(|v| v.into());
        let mut error = None;

        let result = tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => match update(old_value) {
                    Ok(new_value) => new_value.map(|v| v.into()),
                    Err(err) => {
                        error = Some(err);
                        Some(old_value.clone())
                    }
                },
                None => value.take(),
            },
            return_type,
        )?;

        match error {
            Some(err) => Err(err),
            None => Ok(result),
        }
    }

    fn try_update_or_insert_with<K: Into<UserKey>, F1, F2, V: Into<UserValue>>(
        &mut self,
        partition: &OptimisticTxKeyspace,
        key: K,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<Slice>>
    where
        F1: FnOnce(&UserValue) -> Result<Option<V>>,
        F2: FnOnce() -> Result<Option<V>>,
    {
        let mut error = None;

        let result = tx_update(
            self,
            partition,
            key,
            |old_value| match old_value {
                Some(old_value) => match update(old_value) {
                    Ok(new_value) => new_value.map(|v| v.into()),
                    Err(err) => {
                        error = Some(err);
                        Some(old_value.clone())
                    }
                },
                None => match insert() {
                    Ok(new_value) => new_value.map(|v| v.into()),
                    Err(err) => {
                        error = Some(err);
                        None
                    }
                },
            },
            return_type,
        )?;

        match error {
            Some(err) => Err(err),
            None => Ok(result),
        }
    }
}
