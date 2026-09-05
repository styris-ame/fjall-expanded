use std::marker::PhantomData;
use std::ops::RangeBounds;

use anyhow::Result;
use anyhow::anyhow;
use fjall::{Slice, UserValue};
use log::info;
use migrateable::MigrateDataKind;
use migrateable::Migrateable;

use crate::DataGuard;
use crate::schema_map_space::{SpaceSchema, ValidatedSchema};
use crate::versioned_key::{VersionedKey, make_versioned_prefix};
use crate::SchemaMapSpace;
use crate::{ReturnType, versioned_key_slice::ToVersionedKeySlice};

use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, OwnedWriteBatch};

use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{IterExtensions, KeySource, Readable, WriteTxExtensions};

// ── V::CurrentersionedStoreSpace struct ──────────────────────────────────────────────────

pub struct VersionedStoreSpace<K: Migrateable, V: Migrateable>
where
    K::Current: KeySource<K::Current>,
    V::Current: TryFrom<Slice> + TryInto<Slice>,
{
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for VersionedStoreSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: TryFrom<Slice> + TryInto<Slice>,
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

impl<K, V> VersionedStoreSpace<K, V>
where
    K: Migrateable + 'static,
    K::Current: KeySource<K::Current>,
    V: Migrateable + 'static,
    V::Current: TryFrom<Slice> + TryInto<Slice>,
{
    /// Opens the keyspace and validates its stored schema against the current
    /// types, returning the live space with its data migration queued to run
    /// after every space has validated. Validation reads only, so a schema
    /// mismatch here leaves the store untouched.
    pub fn stage_open(group: &mut SpaceGroup, name: impl AsRef<str>, options: SpaceOptions) -> Result<Self> {
        let (space, full_name, validated, schema_map) = Self::open_and_validate(group, name, options)?;
        let migrator = space.clone();
        group.defer_migration(move || migrator.run_migration(&full_name, &validated, &schema_map));
        Ok(space)
    }

    fn open_and_validate(
        group: &mut SpaceGroup,
        name: impl AsRef<str>,
        options: SpaceOptions,
    ) -> Result<(Self, String, ValidatedSchema<K, V>, SchemaMapSpace)> {
        if K::CURRENT_DATA_KIND != MigrateDataKind::Key && K::CURRENT_DATA_KIND != MigrateDataKind::Bytes {
            return Err(anyhow!("Migrateable keys for `VersionedStoreSpace` must use the `Type` or `KeyType` data kind"));
        }

        let full_name = group.full_name(name);
        let keyspace = group.open_keyspace(full_name.clone(), options)?;
        let database = group.database().clone();

        let space = Self {
            inner: keyspace,
            database,
            _marker: PhantomData,
        };
        let validated = group.schema_map().check_space_schema_and_insert_if_empty::<K, V>(&full_name)?;
        let schema_map = group.schema_map().clone();
        Ok((space, full_name, validated, schema_map))
    }

    fn run_migration(&self, full_name: &str, validated: &ValidatedSchema<K, V>, schema_map: &SchemaMapSpace) -> Result<()> {
        self.migrate(full_name, validated)?;
        schema_map.write_space_schema_if_changed(
            full_name,
            &validated.schema,
            &SpaceSchema::from_migrateables(K::iter().collect(), V::iter().collect()),
        )
    }

    fn migrate(&self, space_name: &str, schema: &ValidatedSchema<K, V>) -> Result<()> {
        const BATCH_SIZE: usize = 1024 * 1024 * 64;

        for (key_version, _) in schema.schema.key_schema.iter() {
            if key_version.to_u8() >= K::CURRENT_VERSION || key_version.to_u8() < schema.stored_key_version {
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

        for (value_version, _) in schema.schema.value_schema.iter() {
            if value_version.to_u8() >= V::CURRENT_VERSION || value_version.to_u8() < schema.stored_value_version {
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

impl<'kc, 'vc, K, V> VersionedStoreSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    V: Migrateable,
    V::Current: TryFrom<Slice> + TryInto<Slice>,
    <V::Current as TryFrom<Slice>>::Error: Into<anyhow::Error>,
    V::CurrentLifetimed<'vc>: TryFrom<Slice> + TryInto<Slice>,
    <V::CurrentLifetimed<'vc> as TryFrom<Slice>>::Error: Into<anyhow::Error>,
{
    // ── Readable mirrors ───────────────────────────────────────────

    pub fn get(&self, tx: &impl Readable, key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>) -> Result<Option<V::Current>> {
        match tx.get(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))? {
            Some(v) => Ok(Some(V::Current::try_from(v).map_err(Into::into)?)),
            None => Ok(None),
        }
    }

    pub fn contains_key(
        &self,
        tx: &impl Readable,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<bool> {
        tx.contains_key(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))
    }

    pub fn iter(&self, tx: &impl Readable) -> impl Iterator<Item = Result<(VersionedKey<K::Current>, V::Current)>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::Current::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn range<RK: AsRef<[u8]>, R: RangeBounds<RK>>(
        &self,
        tx: &impl Readable,
        range: R,
    ) -> impl Iterator<Item = Result<(VersionedKey<K::Current>, V::Current)>> {
        tx.range(&self.database, &self.inner, range).map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.into(), V::Current::try_from(value).map_err(Into::into)?))
        })
    }

    pub fn is_empty(&self, tx: &impl Readable) -> fjall::Result<bool> {
        tx.is_empty(&self.database, &self.inner)
    }

    pub fn len(&self, tx: &impl Readable) -> fjall::Result<usize> {
        tx.len(&self.database, &self.inner)
    }

    pub fn size_of(
        &self,
        tx: &impl Readable,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
    ) -> fjall::Result<Option<u32>> {
        tx.size_of(&self.database, &self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))
    }

    pub fn first_key_value(&self, tx: &impl Readable) -> Option<DataGuard<K::Current, V::Current>> {
        tx.first_key_value(&self.inner).map(DataGuard::new)
    }

    pub fn last_key_value(&self, tx: &impl Readable) -> Option<DataGuard<K::Current, V::Current>> {
        tx.last_key_value(&self.inner).map(DataGuard::new)
    }

    // ── IterExtensions mirrors ─────────────────────────────────────

    pub fn iter_guarded(&self, tx: &impl Readable) -> impl Iterator<Item = DataGuard<K::Current, V::Current>> {
        tx.iter(&self.database, &self.inner).map(|g| DataGuard::new(g))
    }

    pub fn iter_keys(&self, tx: &impl Readable) -> impl Iterator<Item = Result<VersionedKey<K::Current>>> {
        tx.iter(&self.database, &self.inner).map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values(&self, tx: &impl Readable) -> impl Iterator<Item = Result<V::Current>> {
        tx.iter(&self.database, &self.inner).map(|guard| {
            let raw = guard.value()?;
            V::Current::try_from(raw).map_err(Into::into)
        })
    }

    pub fn iter_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<(VersionedKey<K::Current>, V::Current)>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| {
                let (key, value) = guard.into_inner()?;
                Ok((key.into(), V::Current::try_from(value).map_err(Into::into)?))
            })
    }

    pub fn iter_guarded_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = DataGuard<K::Current, V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<VersionedKey<K::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .map(|guard| {
                let raw = guard.value()?;
                V::Current::try_from(raw).map_err(Into::into)
            })
    }

    pub fn iter_guarded_rev(&self, tx: &impl Readable) -> impl Iterator<Item = DataGuard<K::Current, V::Current>> {
        tx.iter(&self.database, &self.inner).rev().map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<VersionedKey<K::Current>>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_rev(&self, tx: &impl Readable) -> impl Iterator<Item = Result<V::Current>> {
        tx.iter(&self.database, &self.inner).rev().map(|guard| {
            let raw = guard.value()?;
            V::Current::try_from(raw).map_err(Into::into)
        })
    }

    pub fn iter_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<(VersionedKey<K::Current>, V::Current)>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| {
                let (key, value) = guard.into_inner()?;
                Ok((key.into(), V::Current::try_from(value).map_err(Into::into)?))
            })
    }

    pub fn iter_guarded_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = DataGuard<K::Current, V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|g| DataGuard::new(g))
    }

    pub fn iter_keys_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<VersionedKey<K::Current>>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| Ok(guard.key()?.into()))
    }

    pub fn iter_values_prefixed_rev<P: AsRef<[u8]>>(
        &self,
        tx: &impl Readable,
        prefix: P,
    ) -> impl Iterator<Item = Result<V::Current>> {
        tx.prefix(&self.database, &self.inner, make_versioned_prefix(K::CURRENT_VERSION, V::CURRENT_VERSION, prefix))
            .rev()
            .map(|guard| {
                let raw = guard.value()?;
                V::Current::try_from(raw).map_err(Into::into)
            })
    }
}

// ── Write methods ───────────────────────────────────────────

impl<K, V> VersionedStoreSpace<K, V>
where
    K: Migrateable,
    K::Current: KeySource<K::Current>,
    V: Migrateable,
    V::Current: TryFrom<Slice> + TryInto<Slice>,
    <V::Current as TryFrom<Slice>>::Error: Into<anyhow::Error>,
    <V::Current as TryInto<Slice>>::Error: Into<anyhow::Error>,
{
    // ── Base write methods ─────────────────────────────────────────

    pub fn insert<'kc, 'vc>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        value: impl Into<V::CurrentLifetimed<'vc>>,
    ) -> Result<()>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        V::CurrentLifetimed<'vc>: TryInto<Slice>,
        <V::CurrentLifetimed<'vc> as TryInto<Slice>>::Error: Into<anyhow::Error>,
    {
        let raw: Slice = value.into().try_into().map_err(Into::into)?;
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
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        match tx.take(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION))? {
            Some(raw) => Ok(Some(V::Current::try_from(raw).map_err(Into::into)?)),
            None => Ok(None),
        }
    }

    pub fn update_fetch<'kc, F: FnOnce(Option<V::Current>) -> Option<V::Current>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        f: F,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result =
            tx.update_fetch(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), |old: Option<&UserValue>| {
                let typed_old = match old {
                    Some(v) => match V::Current::try_from(v.clone()) {
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

        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn fetch_update<'kc, F: FnOnce(Option<V::Current>) -> Option<V::Current>>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        f: F,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
    {
        let mut error: Option<anyhow::Error> = None;

        let result =
            tx.fetch_update(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), |old: Option<&UserValue>| {
                let typed_old = match old {
                    Some(v) => match V::Current::try_from(v.clone()) {
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

        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    // ── WriteTxExtensions mirrors ──────────────────────────────────

    pub fn update<'kc, 'vc>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        value: Option<impl Into<V::CurrentLifetimed<'vc>>>,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        V::CurrentLifetimed<'vc>: TryInto<Slice>,
        <V::CurrentLifetimed<'vc> as TryInto<Slice>>::Error: Into<anyhow::Error>,
    {
        let raw_value: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.update(&self.inner, key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION), raw_value, return_type)?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn update_with<'kc, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F: FnOnce(V::Current) -> Option<V::Current>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_with<'kc, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F: FnOnce(V::Current) -> Result<Option<V::Current>>,
    {
        let result = tx.try_update_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn update_or_insert<'kc, 'vc, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        value: Option<impl Into<V::CurrentLifetimed<'vc>>>,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        V::CurrentLifetimed<'vc>: TryInto<Slice>,
        <V::CurrentLifetimed<'vc> as TryInto<Slice>>::Error: Into<anyhow::Error>,
        F: FnOnce(V::Current) -> Option<V::Current>,
    {
        let raw_fallback: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            raw_fallback,
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn update_or_insert_with<'kc, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F1: FnOnce(V::Current) -> Option<V::Current>,
        F2: FnOnce() -> Option<V::Current>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed).map(TryInto::try_into).transpose().map_err(Into::into)
            },
            || -> Result<Option<Slice>> { insert().map(TryInto::try_into).transpose().map_err(Into::into) },
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_or_insert<'kc, 'vc, F>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F,
        value: Option<impl Into<V::CurrentLifetimed<'vc>>>,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        V::CurrentLifetimed<'vc>: TryInto<Slice>,
        <V::CurrentLifetimed<'vc> as TryInto<Slice>>::Error: Into<anyhow::Error>,
        F: FnOnce(V::Current) -> Result<Option<V::Current>>,
    {
        let raw_fallback: Option<Slice> = value.map(Into::into).map(TryInto::try_into).transpose().map_err(Into::into)?;
        let result = tx.try_update_or_insert(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            raw_fallback,
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }

    pub fn try_update_or_insert_with<'kc, F1, F2>(
        &self,
        tx: &mut OptimisticWriteTx,
        key: impl ToVersionedKeySlice<K::CurrentLifetimed<'kc>>,
        update: F1,
        insert: F2,
        return_type: ReturnType,
    ) -> Result<Option<V::Current>>
    where
        K::CurrentLifetimed<'kc>: KeySource<K::CurrentLifetimed<'kc>>,
        F1: FnOnce(V::Current) -> Result<Option<V::Current>>,
        F2: FnOnce() -> Result<Option<V::Current>>,
    {
        let result = tx.try_update_or_insert_with(
            &self.inner,
            key.to_slice(K::CURRENT_VERSION, V::CURRENT_VERSION),
            |old_value: &UserValue| -> Result<Option<Slice>> {
                let typed = V::Current::try_from(old_value.clone()).map_err(Into::into)?;
                update(typed)?.map(TryInto::try_into).transpose().map_err(Into::into)
            },
            || -> Result<Option<Slice>> { insert()?.map(TryInto::try_into).transpose().map_err(Into::into) },
            return_type,
        )?;
        result.map(V::Current::try_from).transpose().map_err(Into::into)
    }
}