use std::marker::PhantomData;
use std::ops::RangeBounds;

use anyhow::Result;
use fjall::{Slice, UserValue};

use crate::ReturnType;
use crate::store_key::SpaceKey;
use crate::store_key::SpaceKeyRef;
use crate::{AliasGuard, key_slice::ToKeySlice};

use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx};

use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{KeySource, Readable, WriteTxExtensions};

// ── AliasGuard ─────────────────────────────────────────────────────────

// ── AliasSpace struct ──────────────────────────────────────────────────

pub struct AliasSpace<K: KeySource<K>, V: KeySource<V>> {
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    _marker: PhantomData<fn() -> (K, V)>,
}


impl<K, V> Clone for AliasSpace<K, V>
where
    K: KeySource<K>,
    V: KeySource<V>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            database: self.database.clone(),
            _marker: PhantomData,
        }
    }
}

// ── Constructors ───────────────────────────────────────────────────────

impl<K, V> AliasSpace<K, V>
where
    K: KeySource<K>,
    V: KeySource<V>,
{
    pub fn open(group: &mut SpaceGroup, name: impl AsRef<str>, options: SpaceOptions) -> Result<Self> {
        let keyspace = group.open_keyspace(group.full_name(name), options)?;
        let database = group.database().clone();
        Ok(Self {
            inner: keyspace,
            database,
            _marker: PhantomData,
        })
    }

    /// Opens the keyspace. An unversioned store has nothing to migrate, so this is
    /// just [`open`](Self::open) under the name its versioned siblings use, letting
    /// a whole store be declared with one consistent call.
    pub fn stage_open(group: &mut SpaceGroup, name: impl AsRef<str>, options: SpaceOptions) -> Result<Self> {
        Self::open(group, name, options)
    }
}

// ── Read methods (Readable + IterExtensions mirrors) ───────────────────

impl<K, V> AliasSpace<K, V>
where
    K: KeySource<K>,
    V: KeySource<V>,
{
    // ── Readable mirrors ───────────────────────────────────────────

    pub fn get(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> Result<Option<SpaceKey<V>>> {
        match tx.get(&self.inner, key.to_slice())? {
            Some(v) => Ok(Some(v.into())),
            None => Ok(None),
        }
    }

    pub fn contains_key(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> fjall::Result<bool> {
        tx.contains_key(&self.inner, key.to_slice())
    }

    pub fn iter(&self, tx: &impl Readable) -> impl Iterator<Item = Result<(SpaceKey<K>, SpaceKey<V>)>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), value.into()))
        })
    }

    pub fn range<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, SpaceKey<V>)>> {
        tx.range(&self.database, &self.inner, range).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), value.into()))
        })
    }

    pub fn is_empty(&self, tx: &impl Readable) -> fjall::Result<bool> {
        tx.is_empty(&self.database, &self.inner)
    }

    pub fn len(&self, tx: &impl Readable) -> fjall::Result<usize> {
        tx.len(&self.database, &self.inner)
    }

    pub fn size_of(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> fjall::Result<Option<u32>> {
        tx.size_of(&self.database, &self.inner, key.to_slice())
    }

    pub fn first_key_value(&self, tx: &impl Readable) -> Option<AliasGuard<K, V>> {
        tx.first_key_value(&self.inner).map(AliasGuard::new)
    }

    pub fn last_key_value(&self, tx: &impl Readable) -> Option<AliasGuard<K, V>> {
        tx.last_key_value(&self.inner).map(AliasGuard::new)
    }

    // ── IterExtensions mirrors ─────────────────────────────────────

    pub fn iter_guarded(&self, tx: &impl Readable) -> impl Iterator<Item = AliasGuard<K, V>> {
        tx.iter(&self.database, &self.inner).map(|g| AliasGuard::new(g))
    }

    pub fn iter_keys(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<V>>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let raw = guard.value()?;
            Ok(raw.into())
        })
    }

    pub fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, SpaceKey<V>)>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), value.into()))
        })
    }

    pub fn iter_guarded_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = AliasGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|g| AliasGuard::new(g))
    }

    pub fn iter_keys_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<SpaceKey<V>>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let raw = guard.value()?;
            Ok(raw.into())
        })
    }

    pub fn iter_guarded_rev(&self, tx: &impl Readable) -> impl Iterator<Item = AliasGuard<K, V>> {
        tx.iter(&self.database, &self.inner).rev().map(|g| AliasGuard::new(g))
    }

    pub fn iter_keys_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<V>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| {
            let raw = guard.value()?;
            Ok(raw.into())
        })
    }

    pub fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, SpaceKey<V>)>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), value.into()))
        })
    }

    pub fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = AliasGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|g| AliasGuard::new(g))
    }

    pub fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<SpaceKey<V>>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let raw = guard.value()?;
            Ok(raw.into())
        })
    }
}

// ── Write methods ───────────────────────────────────────────

impl<K, V> AliasSpace<K, V>
where
    K: KeySource<K>,
    V: KeySource<V> + ToKeySlice<V>,
{
    // ── Base write methods ─────────────────────────────────────────

    pub fn insert(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>, value: impl ToKeySlice<V>) -> Result<()> {
        let raw: Slice = value.to_slice();
        tx.insert(&self.inner, key.to_slice(), raw);
        Ok(())
    }

    pub fn remove(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) {
        tx.remove(&self.inner, key.to_slice());
    }

    pub fn take(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) -> Result<Option<SpaceKey<V>>> {
        match tx.take(&self.inner, key.to_slice())? {
            Some(raw) => Ok(Some(raw.into())),
            None => Ok(None),
        }
    }

    pub fn update_fetch<F: FnOnce(Option<SpaceKeyRef<V>>) -> Option<V>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<SpaceKey<V>>> {
        let result = tx.update_fetch(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old: Option<SpaceKeyRef<V>> = old.map(|v| v.into());

            f(typed_old).map(|new_val| new_val.to_slice())
        })?;

        Ok(result.map(|slice| slice.into()))
    }

    pub fn fetch_update<F: FnOnce(Option<SpaceKeyRef<V>>) -> Option<V>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<SpaceKey<V>>> {
        let result = tx.fetch_update(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old: Option<SpaceKeyRef<V>> = old.map(|v| v.into());

            f(typed_old).map(|new_val| new_val.to_slice())
        })?;

        Ok(result.map(|slice| slice.into()))
    }

    // ── WriteTxExtensions mirrors ──────────────────────────────────

    pub fn update(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        value: Option<impl ToKeySlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>> {
        let raw_value: Option<Slice> = value.map(|v| v.to_slice());
        let result = tx.update(&self.inner, key.to_slice(), raw_value, return_type)?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn update_with<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F: FnOnce(SpaceKeyRef<V>) -> Option<V>,
    {
        let result = tx.update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Option<Slice> {
                let typed: SpaceKeyRef<V> = old_value.into();
                update(typed).map(|v| v.to_slice())
            },
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn try_update_with<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F: FnOnce(SpaceKeyRef<V>) -> Result<Option<V>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed: SpaceKeyRef<V> = old_value.into();
                Ok(update(typed)?.map(|v| v.to_slice()))
            },
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn update_or_insert<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl ToKeySlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F: FnOnce(SpaceKeyRef<V>) -> Option<V>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.to_slice());
        let result = tx.update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Option<Slice> {
                let typed: SpaceKeyRef<V> = old_value.into();
                update(typed).map(|v| v.to_slice())
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn update_or_insert_with<F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F1: FnOnce(SpaceKeyRef<V>) -> Option<V>,
        F2: FnOnce() -> Option<V>,
    {
        let result = tx.update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Option<Slice> {
                let typed: SpaceKeyRef<V> = old_value.into();
                update(typed).map(|v| v.to_slice())
            },
            || -> Option<Slice> { insert().map(|v| v.to_slice()) },
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn try_update_or_insert<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl ToKeySlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F: FnOnce(SpaceKeyRef<V>) -> Result<Option<V>>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.to_slice());
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed: SpaceKeyRef<V> = old_value.into();
                Ok(update(typed)?.map(|v| v.to_slice()))
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }

    pub fn try_update_or_insert_with<F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<SpaceKey<V>>>
    where
        F1: FnOnce(SpaceKeyRef<V>) -> Result<Option<V>>,
        F2: FnOnce() -> Result<Option<V>>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed: SpaceKeyRef<V> = old_value.into();
                Ok(update(typed)?.map(|v| v.to_slice()))
            },
            || -> Result<Option<Slice>> { Ok(insert()?.map(|v| v.to_slice())) },
            return_type,
        )?;
        Ok(result.map(|slice| slice.into()))
    }
}
