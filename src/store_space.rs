use std::marker::PhantomData;
use std::ops::RangeBounds;

use anyhow::Result;
use fjall::{Slice, UserValue};

use crate::key_slice::ToKeySlice;
use crate::store_key::SpaceKey;
use crate::{DataGuard, ReturnType};

use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx};

use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{KeySource, Readable, WriteTxExtensions};

// ── StoreSpace struct ──────────────────────────────────────────────────

pub struct StoreSpace<K: KeySource<K>, V: TryFrom<Slice> + TryInto<Slice>> {
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for StoreSpace<K, V>
where
    K: KeySource<K>,
    V: TryFrom<Slice> + TryInto<Slice>,
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

impl<K, V> StoreSpace<K, V>
where
    K: KeySource<K>,
    V: TryFrom<Slice> + TryInto<Slice>,
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

impl<K, V> StoreSpace<K, V>
where
    K: KeySource<K>,
    V: TryFrom<Slice> + TryInto<Slice>,
    <V as TryFrom<Slice>>::Error: Into<anyhow::Error>,
{
    // ── Readable mirrors ───────────────────────────────────────────

    pub fn get(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> Result<Option<V>> {
        match tx.get(&self.inner, key.to_slice())? {
            Some(v) => Ok(Some(V::try_from(v).map_err(Into::into)?)),
            None => Ok(None),
        }
    }

    pub fn contains_key(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> fjall::Result<bool> {
        tx.contains_key(&self.inner, key.to_slice())
    }

    pub fn iter(&self, tx: &impl Readable) -> impl Iterator<Item = Result<(SpaceKey<K>, V)>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.inner.inner().approximate_len()
    }

    pub fn disk_space(&self) -> u64 {
        self.inner.inner().disk_space()
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

    pub fn first_key_value(&self, tx: &impl Readable) -> Option<DataGuard<K, V>> {
        tx.first_key_value(&self.inner).map(DataGuard::new)
    }

    pub fn last_key_value(&self, tx: &impl Readable) -> Option<DataGuard<K, V>> {
        tx.last_key_value(&self.inner).map(DataGuard::new)
    }

    // ── IterExtensions mirrors ─────────────────────────────────────

    pub fn iter_guarded(&self, tx: &impl Readable) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.iter(&self.database, &self.inner).map(|g| DataGuard::new(g))
    }

    pub fn iter_keys(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values(&self, tx: &impl Readable) -> impl Iterator<Item = Result<V>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }

    pub fn iter_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = Result<(SpaceKey<K>, V)>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn iter_guarded_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = Result<V>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }

    pub fn iter_guarded_rev(&self, tx: &impl Readable) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.iter(&self.database, &self.inner).rev().map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<V>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }

    pub fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, V)>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed_rev<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = Result<V>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }

    // ── Range scans ────────────────────────────────────────────────

    pub fn range<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, V)>> {
        tx.range(&self.database, &self.inner, range).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn range_guarded<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.range(&self.database, &self.inner, range).map(|g| DataGuard::new(g))
    }

    pub fn range_keys<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.range(&self.database, &self.inner, range).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn range_values<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<V>> {
        tx.range(&self.database, &self.inner, range).map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }

    pub fn range_rev<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<(SpaceKey<K>, V)>> {
        tx.range(&self.database, &self.inner, range).rev().map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn range_guarded_rev<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = DataGuard<K, V>> {
        tx.range(&self.database, &self.inner, range).rev().map(|g| DataGuard::new(g))
    }

    pub fn range_keys_rev<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<SpaceKey<K>>> {
        tx.range(&self.database, &self.inner, range).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn range_values_rev<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<V>> {
        tx.range(&self.database, &self.inner, range).rev().map(|guard| {
            let raw = guard.value()?;
            V::try_from(raw).map_err(Into::into)
        })
    }
}

// ── Optimistic write methods ───────────────────────────────────────────

impl<K, V> StoreSpace<K, V>
where
    K: KeySource<K>,
    V: TryFrom<Slice> + TryInto<Slice>,
    <V as TryFrom<Slice>>::Error: Into<anyhow::Error>,
    <V as TryInto<Slice>>::Error: Into<anyhow::Error>,
{
    // ── Base write methods ─────────────────────────────────────────

    pub fn insert(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>, value: impl Into<V>) -> Result<()> {
        let raw: Slice = value.into().try_into().map_err(Into::into)?;
        tx.insert(&self.inner, key.to_slice(), raw);
        Ok(())
    }

    pub fn remove(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) {
        tx.remove(&self.inner, key.to_slice());
    }

    pub fn take(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) -> Result<Option<V>> {
        match tx.take(&self.inner, key.to_slice())? {
            Some(raw) => Ok(Some(V::try_from(raw).map_err(Into::into)?)),
            None => Ok(None),
        }
    }

    pub fn update_fetch<F: FnOnce(Option<V>) -> Option<V>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<V>> {
        let mut error: Option<anyhow::Error> = None;

        let result = tx.update_fetch(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old = match old {
                Some(v) => match V::try_from(v.clone()) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        error = Some(e.into());
                        return old.cloned();
                    }
                },
                None => None,
            };

            match f(typed_old) {
                Some(new_val) => match new_val.try_into() {
                    Ok(raw) => Some(raw),
                    Err(e) => {
                        error = Some(e.into());
                        old.cloned()
                    }
                },
                None => None,
            }
        })?;

        if let Some(e) = error {
            return Err(e);
        }

        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn fetch_update<F: FnOnce(Option<V>) -> Option<V>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<V>> {
        let mut error: Option<anyhow::Error> = None;

        let result = tx.fetch_update(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old = match old {
                Some(v) => match V::try_from(v.clone()) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        error = Some(e.into());
                        return old.cloned();
                    }
                },
                None => None,
            };

            match f(typed_old) {
                Some(new_val) => match new_val.try_into() {
                    Ok(raw) => Some(raw),
                    Err(e) => {
                        error = Some(e.into());
                        old.cloned()
                    }
                },
                None => None,
            }
        })?;

        if let Some(e) = error {
            return Err(e);
        }

        result.map(V::try_from).transpose().map_err(Into::into)
    }

    // ── WriteTxExtensions mirrors ──────────────────────────────────

    pub fn update(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        value: Option<impl Into<V>>,
        return_type: ReturnType,
    ) -> Result<Option<V>> {
        let raw_value: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.update(&self.inner, key.to_slice(), raw_value, return_type)?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn update_with<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F: FnOnce(V) -> Option<V>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_with<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F: FnOnce(V) -> Result<Option<V>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn update_or_insert<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl Into<V>>,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F: FnOnce(V) -> Option<V>,
    {
        let raw_fallback: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            raw_fallback,
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn update_or_insert_with<F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F1: FnOnce(V) -> Option<V>,
        F2: FnOnce() -> Option<V>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            || -> Result<Option<Slice>> { insert().map(TryInto::try_into).transpose().map_err(Into::into) },
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_or_insert<F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl Into<V>>,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F: FnOnce(V) -> Result<Option<V>>,
    {
        let raw_fallback: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            raw_fallback,
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_or_insert_with<F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<V>>
    where
        F1: FnOnce(V) -> Result<Option<V>>,
        F2: FnOnce() -> Result<Option<V>>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            || -> Result<Option<Slice>> { insert()?.map(TryInto::try_into).transpose().map_err(Into::into) },
            return_type,
        )?;
        result.map(V::try_from).transpose().map_err(Into::into)
    }
}