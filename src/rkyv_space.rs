use std::marker::PhantomData;
use std::ops::RangeBounds;

use anyhow::Result;
use bytes::Bytes;
use easy_rkyv::{Rkyv, TrustedRkyv, TrustedRkyvImpl, TrustedRkyvRef};
use fjall::{Slice, UserValue};
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::{Archive, Deserialize, Portable, Serialize, rancor};

use crate::key_slice::ToKeySlice;
use crate::store_key::SpaceKey;
use crate::{ReturnType, RkyvGuard};

use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx};

use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{KeySource, Readable, WriteTxExtensions};

// ── TryToRkyvSlice ─────────────────────────────────────────────────────

pub trait TryToRkyvSlice<V> {
    fn try_to_slice(self) -> Result<Slice>;
}

impl<V> TryToRkyvSlice<V> for TrustedRkyv<V, Slice>
where
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
{
    fn try_to_slice(self) -> Result<Slice> {
        Ok(self.into_bytes())
    }
}

impl<V> TryToRkyvSlice<V> for TrustedRkyv<V, Vec<u8>>
where
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
{
    #[cfg(feature = "bytes")]
    fn try_to_slice(self) -> Result<Slice> {
        Ok(self.into_bytes().into())
    }
    #[cfg(not(feature = "bytes"))]
    fn try_to_slice(self) -> Result<Slice> {
        Ok(self.into_bytes().to_vec().into())
    }
}

impl<V> TryToRkyvSlice<V> for TrustedRkyv<V, Bytes>
where
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
{
    fn try_to_slice(self) -> Result<Slice> {
        Ok(self.into_bytes().into())
    }
}

impl<'a, V> TryToRkyvSlice<V> for TrustedRkyvRef<'a, V>
where
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'b> CheckBytes<HighValidator<'b, rancor::Error>>,
{
    fn try_to_slice(self) -> Result<Slice> {
        Ok(TrustedRkyvImpl::bytes(&self).into())
    }
}

impl<V> TryToRkyvSlice<V> for V
where
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    fn try_to_slice(self) -> Result<Slice> {
        Ok(Rkyv::serialize(&self)?.into())
    }
}

// ── RkyvSpace struct ──────────────────────────────────────────────────

pub struct RkyvSpace<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for RkyvSpace<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
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

impl<K, V> RkyvSpace<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
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

impl<K, V> RkyvSpace<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    // ── Readable mirrors ───────────────────────────────────────────

    pub fn get(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> fjall::Result<Option<TrustedRkyv<V>>> {
        match tx.get(&self.inner, key.to_slice())? {
            Some(v) => Ok(Some(TrustedRkyv::new(v))),
            None => Ok(None),
        }
    }

    pub fn contains_key(&self, tx: &impl Readable, key: impl ToKeySlice<K>) -> fjall::Result<bool> {
        tx.contains_key(&self.inner, key.to_slice())
    }

    pub fn iter(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<(SpaceKey<K>, TrustedRkyv<V>)>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), TrustedRkyv::new(value)))
        })
    }

    pub fn range<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = fjall::Result<(SpaceKey<K>, TrustedRkyv<V>)>> {
        tx.range(&self.database, &self.inner, range).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), TrustedRkyv::new(value)))
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

    pub fn first_key_value(&self, tx: &impl Readable) -> Option<RkyvGuard<K, V>> {
        tx.first_key_value(&self.inner).map(RkyvGuard::new)
    }

    pub fn last_key_value(&self, tx: &impl Readable) -> Option<RkyvGuard<K, V>> {
        tx.last_key_value(&self.inner).map(RkyvGuard::new)
    }

    // ── IterExtensions mirrors ─────────────────────────────────────

    pub fn iter_guarded(&self, tx: &impl Readable) -> impl Iterator<Item = RkyvGuard<K, V>> {
        tx.iter(&self.database, &self.inner).map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V>>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }

    pub fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<(SpaceKey<K>, TrustedRkyv<V>)>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), TrustedRkyv::new(value)))
        })
    }

    pub fn iter_guarded_prefixed<P: AsRef<[u8]>>(&self, tx: &impl Readable, prefix: P) -> impl Iterator<Item = RkyvGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V>>> {
        tx.prefix(&self.database, &self.inner, prefix).map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }

    pub fn iter_guarded_rev(&self, tx: &impl Readable) -> impl Iterator<Item = RkyvGuard<K, V>> {
        tx.iter(&self.database, &self.inner).rev().map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_rev(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<SpaceKey<K>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_rev(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }

    pub fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<(SpaceKey<K>, TrustedRkyv<V>)>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), TrustedRkyv::new(value)))
        })
    }

    pub fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = RkyvGuard<K, V>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<SpaceKey<K>>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V>>> {
        tx.prefix(&self.database, &self.inner, prefix).rev().map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }
}

// ── Write methods ───────────────────────────────────────────

impl<K, V> RkyvSpace<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    // ── Base write methods ─────────────────────────────────────────

    pub fn insert(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>, value: impl TryToRkyvSlice<V>) -> Result<()> {
        let raw = value.try_to_slice()?;
        tx.insert(&self.inner, key.to_slice(), raw);
        Ok(())
    }

    pub fn remove(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) {
        tx.remove(&self.inner, key.to_slice());
    }

    pub fn take(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K>) -> fjall::Result<Option<TrustedRkyv<V>>> {
        match tx.take(&self.inner, key.to_slice())? {
            Some(raw) => Ok(Some(TrustedRkyv::new(raw))),
            None => Ok(None),
        }
    }

    pub fn update_fetch<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(Option<TrustedRkyvRef<'a, V>>) -> Option<R>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result = tx.update_fetch(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old = old.map(|v| TrustedRkyvRef::<V>::new(v));

            match f(typed_old) {
                Some(new_val) => match new_val.try_to_slice() {
                    Ok(raw) => Some(raw),
                    Err(e) => {
                        error = Some(e);
                        old.cloned()
                    }
                },
                None => None,
            }
        })?;

        if let Some(e) = error {
            return Err(e);
        }

        Ok(result.map(TrustedRkyv::new))
    }

    pub fn fetch_update<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        f: F,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(Option<TrustedRkyvRef<'a, V>>) -> Option<R>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result = tx.fetch_update(&self.inner, key.to_slice(), |old: Option<&UserValue>| {
            let typed_old = old.map(|v| TrustedRkyvRef::<V>::new(v));

            match f(typed_old) {
                Some(new_val) => match new_val.try_to_slice() {
                    Ok(raw) => Some(raw),
                    Err(e) => {
                        error = Some(e);
                        old.cloned()
                    }
                },
                None => None,
            }
        })?;

        if let Some(e) = error {
            return Err(e);
        }

        Ok(result.map(TrustedRkyv::new))
    }

    // ── WriteTxExtensions mirrors ──────────────────────────────────

    pub fn update(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        value: Option<impl TryToRkyvSlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>> {
        let raw_value: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.update(&self.inner, key.to_slice(), raw_value, return_type)?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_with<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Option<R>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_with<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Result<Option<R>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_or_insert<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl TryToRkyvSlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Option<R>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_or_insert_with<R1: TryToRkyvSlice<V>, R2: TryToRkyvSlice<V>, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F1: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Option<R1>,
        F2: FnOnce() -> Option<R2>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            || -> Result<Option<Slice>> { insert().map(|r| r.try_to_slice()).transpose() },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_or_insert<R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F,
        value: Option<impl TryToRkyvSlice<V>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Result<Option<R>>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_or_insert_with<R1: TryToRkyvSlice<V>, R2: TryToRkyvSlice<V>, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToKeySlice<K>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V>>>
    where
        F1: for<'a> FnOnce(TrustedRkyvRef<'a, V>) -> Result<Option<R1>>,
        F2: FnOnce() -> Result<Option<R2>>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            || -> Result<Option<Slice>> { insert()?.map(|r| r.try_to_slice()).transpose() },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }
}