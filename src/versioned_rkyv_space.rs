use std::marker::PhantomData;
use std::ops::RangeBounds;

use anyhow::Result;
use anyhow::anyhow;
use bytes::Bytes;
use easy_rkyv::{Rkyv, TrustedRkyv, TrustedRkyvImpl, TrustedRkyvRef};
use fjall::{Guard, Slice, UserValue};
use log::info;
use migrateable::MigrateDataKind;
use migrateable::Migrateable;
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::{Archive, Deserialize, Portable, Serialize, rancor};

use crate::schema_map_space::{SpaceSchema, SpaceSchemaHandle};
use crate::versioned_key::make_versioned_prefix;
use crate::versioned_key_slice::ToVersionedKeySlice;
use crate::{KeySource, ReturnType, VersionedKey};

use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, OwnedWriteBatch};

use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{IterExtensions, Readable, WriteTxExtensions};

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
    fn try_to_slice(self) -> Result<Slice> {
        Ok(self.into_bytes().into())
    }
}

impl<V> TryToRkyvSlice<V> for TrustedRkyv<V, Bytes>
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

impl<'a, V> TryToRkyvSlice<V> for TrustedRkyvRef<'_, V>
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

impl<V> TryToRkyvSlice<V> for &V
where
    V: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    fn try_to_slice(self) -> Result<Slice> {
        Ok(Rkyv::serialize(self)?.into())
    }
}

// ── RkyvGuard ──────────────────────────────────────────────────────────

pub struct RkyvGuard<K, V> {
    inner: Guard,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> RkyvGuard<K, V>
where
    K: KeySource<K>,
    V: Archive,
    V::Archived:
        Portable + Deserialize<V, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
{
    fn new(guard: Guard) -> Self {
        Self {
            inner: guard,
            _marker: PhantomData,
        }
    }

    pub fn key(self) -> fjall::Result<VersionedKey<K>> {
        self.inner.key().map(Into::into)
    }

    pub fn value(self) -> fjall::Result<TrustedRkyv<V>> {
        let raw = self.inner.value()?;
        Ok(TrustedRkyv::new(raw))
    }

    pub fn into_inner(self) -> fjall::Result<(VersionedKey<K>, TrustedRkyv<V>)> {
        let (key, value) = self.inner.into_inner()?;
        Ok((key.into(), TrustedRkyv::new(value)))
    }

    pub fn raw(&self) -> &Guard {
        &self.inner
    }

    pub fn into_raw(self) -> Guard {
        self.inner
    }
}

// ── VersionedRkyvSpace struct ──────────────────────────────────────────────────

pub struct VersionedRkyvSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: Archive,
    <V::Current as Archive>::Archived: Portable
        + Deserialize<V::Current, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::Current: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for VersionedRkyvSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: Archive,
    <V::Current as Archive>::Archived: Portable
        + Deserialize<V::Current, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::Current: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
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

impl<'kc, 'vc, K, V> VersionedRkyvSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    V: Migrateable,
    V::Current: Archive,
    <V::Current as Archive>::Archived: Portable
        + Deserialize<V::Current, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::Current: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
    V::CurrentLifetimed<'vc>: Archive,
    <V::CurrentLifetimed<'vc> as Archive>::Archived: Portable
        + Deserialize<V::CurrentLifetimed<'vc>, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::CurrentLifetimed<'vc>: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    pub fn open_and_migrate(group: &mut SpaceGroup, name: impl AsRef<str>, options: SpaceOptions) -> Result<Self> {
        if K::CURRENT_DATA_KIND != MigrateDataKind::Key && K::CURRENT_DATA_KIND != MigrateDataKind::Bytes {
            return Err(anyhow!("Migrateable keys for `VersionedRkyvSpace` must use the `Type` or `KeyType` data kind"));
        }
        if V::CURRENT_DATA_KIND != MigrateDataKind::Rkyv {
            return Err(anyhow!("Current migrateable value for `VersionedRkyvSpace` must use the `Rkyv` data kind"));
        }

        let full_name = group.full_name(name);
        let keyspace = group.open_keyspace(full_name.clone(), options)?;
        let database = group.database().clone();

        let result = Self {
            inner: keyspace,
            database,
            _marker: PhantomData,
        };
        let schema_handle = group.schema_map().check_space_schema_and_insert_if_empty::<K, V>(&full_name)?;

        result.migrate(&full_name, &schema_handle)?;

        schema_handle
            .update_space_schema(&full_name, &SpaceSchema::from_migrateables(K::iter().collect(), V::iter().collect()))?;

        Ok(result)
    }

    fn migrate(&self, space_name: &str, schema: &SpaceSchemaHandle<'_, K, V>) -> Result<()> {
        const BATCH_SIZE: usize = 1024 * 1024 * 64;

        for (key_version, _) in schema.schema().key_schema.iter() {
            if key_version.to_u8() >= K::CURRENT_VERSION || key_version.to_u8() < schema.stored_key_version() {
                continue;
            }

            let mut logged = false;

            loop {
                let mut results_size = 0;
                let mut results = Vec::with_capacity(1024);
                for kv in self.database.read_tx().iter_prefixed(&self.database, &self.inner, [key_version.to_u8()]) {
                    if !logged {
                        info!("Migrating keys in '{space_name}' from version {} to {}", key_version.to_u8(), K::CURRENT_VERSION);
                        logged = true;
                    }

                    let (key, value) = kv?;

                    results_size += (1024 * 16) + (key.len() + value.len());
                    results.push((key, value));

                    if results_size >= BATCH_SIZE {
                        break;
                    }
                }

                if results.is_empty() {
                    break;
                }

                let mut tx = OwnedWriteBatch::with_capacity(self.database.inner().clone(), results.len() * 2);

                for (key, mut value) in results.into_iter() {
                    let value_version = V::from_u8(key[1]).ok_or(anyhow!("Expected valid value version in migrating key"))?;
                    if value_version.to_u8() < V::CURRENT_VERSION {
                        value = value_version.migrate(value)?;
                    }

                    let migrated: Vec<u8> = key_version.migrate(&key[2..])?;
                    let mut new_key = Vec::with_capacity(migrated.len() + 2);
                    new_key.push(K::CURRENT_VERSION);
                    new_key.push(V::CURRENT_VERSION);
                    new_key.extend_from_slice(&migrated);

                    tx.remove(self.inner.inner(), key);
                    tx.insert(self.inner.inner(), new_key, value);
                }

                tx.commit()?;
                crate::wait_for_write_buffer(self.database.inner());
            }
        }

        for (value_version, _) in schema.schema().value_schema.iter() {
            if value_version.to_u8() >= V::CURRENT_VERSION || value_version.to_u8() < schema.stored_value_version() {
                continue;
            }

            let mut logged = false;

            loop {
                let mut results_size = 0;
                let mut results = Vec::with_capacity(1024);
                for kv in self.database.read_tx().iter_prefixed(
                    &self.database,
                    &self.inner,
                    [K::CURRENT_VERSION, value_version.to_u8()],
                ) {
                    if !logged {
                        info!(
                            "Migrating values in '{space_name}' from version {} to {}",
                            value_version.to_u8(),
                            V::CURRENT_VERSION
                        );
                        logged = true;
                    }

                    let (key, value) = kv?;

                    results_size += (1024 * 16) + (key.len() + value.len());
                    results.push((key, value));

                    if results_size >= BATCH_SIZE {
                        break;
                    }
                }

                if results.is_empty() {
                    break;
                }

                let mut tx = OwnedWriteBatch::with_capacity(self.database.inner().clone(), results.len() * 2);

                for (key, value) in results.into_iter() {
                    let mut new_key = key.to_vec();
                    new_key[1] = V::CURRENT_VERSION;

                    let new_value: Vec<u8> = value_version.migrate(&value)?;

                    tx.remove(self.inner.inner(), key);
                    tx.insert(self.inner.inner(), new_key, new_value);
                }

                tx.commit()?;
                crate::wait_for_write_buffer(self.database.inner());
            }
        }

        Ok(())
    }
}

// ── Read methods (Readable + IterExtensions mirrors) ───────────────────

impl<K, V> VersionedRkyvSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: Archive,
    <V::Current as Archive>::Archived: Portable
        + Deserialize<V::Current, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::Current: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    // ── Readable mirrors ───────────────────────────────────────────

    pub fn get<'kc>(
        &self,
        tx: &impl Readable,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<Option<TrustedRkyv<V::Current>>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        match tx.get(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))? {
            Some(v) => Ok(Some(TrustedRkyv::new(v))),
            None => Ok(None),
        }
    }

    pub fn contains_key<'kc>(
        &self,
        tx: &impl Readable,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<bool>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        tx.contains_key(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))
    }

    pub fn iter(
        &self,
        tx: &impl Readable,
    ) -> impl Iterator<Item = fjall::Result<(VersionedKey<K::Current>, TrustedRkyv<V::Current>)>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), TrustedRkyv::new(value)))
        })
    }

    pub fn range<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = fjall::Result<(VersionedKey<K::Current>, TrustedRkyv<V::Current>)>> {
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

    pub fn size_of<'kc>(
        &self,
        tx: &impl Readable,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<Option<u32>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        tx.size_of(&self.database, &self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))
    }

    pub fn first_key_value(&self, tx: &impl Readable) -> Option<RkyvGuard<K::Current, V::Current>> {
        tx.first_key_value(&self.inner).map(RkyvGuard::new)
    }

    pub fn last_key_value(&self, tx: &impl Readable) -> Option<RkyvGuard<K::Current, V::Current>> {
        tx.last_key_value(&self.inner).map(RkyvGuard::new)
    }

    // ── IterExtensions mirrors ─────────────────────────────────────

    pub fn iter_guarded(&self, tx: &impl Readable) -> impl Iterator<Item = RkyvGuard<K::Current, V::Current>> {
        tx.iter(&self.database, &self.inner).map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<VersionedKey<K::Current>>> {
        tx.iter(&self.database, &self.inner).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V::Current>>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }

    pub fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<(VersionedKey<K::Current>, TrustedRkyv<V::Current>)>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| {
                let (key, value) = guard.into_inner()?;
                Ok((key.into(), TrustedRkyv::new(value)))
            })
    }

    pub fn iter_guarded_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = RkyvGuard<K::Current, V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<VersionedKey<K::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| {
                let raw = guard.value()?;
                Ok(TrustedRkyv::new(raw))
            })
    }

    pub fn iter_guarded_rev(&self, tx: &impl Readable) -> impl Iterator<Item = RkyvGuard<K::Current, V::Current>> {
        tx.iter(&self.database, &self.inner).rev().map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_rev(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<VersionedKey<K::Current>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_rev(&self, tx: &impl Readable) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V::Current>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| {
            let raw = guard.value()?;
            Ok(TrustedRkyv::new(raw))
        })
    }

    pub fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<(VersionedKey<K::Current>, TrustedRkyv<V::Current>)>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| {
                let (key, value) = guard.into_inner()?;
                Ok((key.into(), TrustedRkyv::new(value)))
            })
    }

    pub fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = RkyvGuard<K::Current, V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|g| RkyvGuard::new(g))
    }

    pub fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<VersionedKey<K::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = fjall::Result<TrustedRkyv<V::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| {
                let raw = guard.value()?;
                Ok(TrustedRkyv::new(raw))
            })
    }
}

// ── Write methods ───────────────────────────────────────────

impl<K, V> VersionedRkyvSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: Archive,
    <V::Current as Archive>::Archived: Portable
        + Deserialize<V::Current, HighDeserializer<rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
    V::Current: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
{
    // ── Base write methods ─────────────────────────────────────────

    pub fn insert<'kc, 'vc>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        value: impl TryToRkyvSlice<V::CurrentLifetimed<'vc>>,
    ) -> Result<()>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        V::CurrentLifetimed<'vc>: Archive,
        <V::CurrentLifetimed<'vc> as Archive>::Archived: Portable
            + Deserialize<V::CurrentLifetimed<'vc>, HighDeserializer<rancor::Error>>
            + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
        V::CurrentLifetimed<'vc>: for<'a> Serialize<HighSerializer<Vec<u8>, ArenaHandle<'a>, rancor::Error>>,
    {
        let raw = value.try_to_slice()?;
        tx.insert(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), raw);
        Ok(())
    }

    pub fn remove<'kc>(&self, tx: &mut OptimisticWriteTx, key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>)
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        tx.remove(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION));
    }

    pub fn take<'kc>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<Option<TrustedRkyv<V::Current>>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        match tx.take(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))? {
            Some(raw) => Ok(Some(TrustedRkyv::new(raw))),
            None => Ok(None),
        }
    }

    pub fn update_fetch<'kc, 'vc, R: TryToRkyvSlice<V::CurrentLifetimed<'vc>>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        f: F,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F: for<'a> FnOnce(Option<TrustedRkyvRef<'a, V::Current>>) -> Option<R>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result =
            tx.update_fetch(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), |old: Option<&UserValue>| {
                let typed_old = old.map(|v| TrustedRkyvRef::<V::Current>::new(v));

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

    pub fn fetch_update<'kc, R: TryToRkyvSlice<V>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        f: F,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F: for<'a> FnOnce(Option<TrustedRkyvRef<'a, V::Current>>) -> Option<R>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result =
            tx.fetch_update(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), |old: Option<&UserValue>| {
                let typed_old = old.map(|v| TrustedRkyvRef::<V::Current>::new(v));

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

    pub fn update<'kc>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        value: Option<impl TryToRkyvSlice<V::Current>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let raw_value: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.update(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), raw_value, return_type)?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_with<'kc, R: TryToRkyvSlice<V::Current>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Option<R>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_with<'kc, R: TryToRkyvSlice<V::Current>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Result<Option<R>>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_or_insert<'kc, R: TryToRkyvSlice<V::Current>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        value: Option<impl TryToRkyvSlice<V::Current>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Option<R>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn update_or_insert_with<'kc, R1: TryToRkyvSlice<V::Current>, R2: TryToRkyvSlice<V::Current>, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F1: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Option<R1>,
        F2: FnOnce() -> Option<R2>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed).map(|r| r.try_to_slice()).transpose()
            },
            || -> Result<Option<Slice>> { insert().map(|r| r.try_to_slice()).transpose() },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_or_insert<'kc, R: TryToRkyvSlice<V::Current>, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        value: Option<impl TryToRkyvSlice<V::Current>>,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Result<Option<R>>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let raw_fallback: Option<Slice> = value.map(|v| v.try_to_slice()).transpose()?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            raw_fallback,
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }

    pub fn try_update_or_insert_with<'kc, R1: TryToRkyvSlice<V::Current>, R2: TryToRkyvSlice<V::Current>, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<TrustedRkyv<V::Current>>>
    where
        F1: for<'a> FnOnce(TrustedRkyvRef<'a, V::Current>) -> Result<Option<R1>>,
        F2: FnOnce() -> Result<Option<R2>>,
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = TrustedRkyvRef::<V::Current>::new(old_value);
                update(typed)?.map(|r| r.try_to_slice()).transpose()
            },
            || -> Result<Option<Slice>> { insert()?.map(|r| r.try_to_slice()).transpose() },
            return_type,
        )?;
        Ok(result.map(TrustedRkyv::new))
    }
}