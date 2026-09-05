use std::borrow::Cow;
use std::{array::TryFromSliceError, collections::HashMap};

use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Slice};
use migrateable::MigrateDataKind;
use migrateable::Migrateable;

use crate::{IterExtensions, SpaceKey, ToKeySlice, encode_key_prefix};

pub(crate) const SCHEMA_MAP_KEYSPACE: &str = "__fjall_expanded_migrateable_schema";

#[derive(Clone)]
pub struct SchemaMapSpace {
    inner: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
}

#[derive(storekey::Encode, storekey::BorrowDecode, crate::SpaceKey)]
struct SchemaKey<'a> {
    key: Cow<'a, str>,
    schema_type: SchemaType,
    data_kind: u8,
    version: u8,
}

pub(crate) struct SpaceSchema<K: Migrateable, V: Migrateable> {
    pub key_schema: Vec<(K, u128)>,
    pub value_schema: Vec<(V, u128)>,
}
impl<K: Migrateable, V: Migrateable> SpaceSchema<K, V> {
    pub fn from_migrateables(key_schema: Vec<K>, value_schema: Vec<V>) -> Self {
        Self {
            key_schema: key_schema
                .into_iter()
                .map(|k| {
                    let hash = k.type_hash();
                    (k, hash)
                })
                .collect(),
            value_schema: value_schema
                .into_iter()
                .map(|v| {
                    let hash = v.type_hash();
                    (v, hash)
                })
                .collect(),
        }
    }

    pub fn new(key_schema: Vec<(K, u128)>, value_schema: Vec<(V, u128)>) -> Self {
        Self {
            key_schema,
            value_schema,
        }
    }
}

/// The stored schema of a space that has passed validation, owned so it can
/// outlive the borrow of the [`SchemaMapSpace`] and travel into a deferred
/// migration. The two version fields are the highest key and value versions
/// found on disk, which bound how far back a migration has to scan.
pub(crate) struct ValidatedSchema<K: Migrateable, V: Migrateable> {
    pub schema: SpaceSchema<K, V>,
    pub stored_key_version: u8,
    pub stored_value_version: u8,
}

impl<K: Migrateable, V: Migrateable> ValidatedSchema<K, V> {
    fn new(schema: SpaceSchema<K, V>) -> Self {
        let stored_key_version = schema.key_schema.iter().map(|(k, _)| k.to_u8()).max().unwrap_or(0);
        let stored_value_version = schema.value_schema.iter().map(|(v, _)| v.to_u8()).max().unwrap_or(0);

        Self {
            schema,
            stored_key_version,
            stored_value_version,
        }
    }
}

#[derive(storekey::Encode, storekey::BorrowDecode)]
#[repr(u8)]
enum SchemaType {
    Key,
    Value,
}

impl SchemaMapSpace {
    pub fn open(database: &OptimisticTxDatabase) -> fjall::Result<Self> {
        let keyspace = database.keyspace(SCHEMA_MAP_KEYSPACE, || KeyspaceCreateOptions::default())?;
        Ok(Self {
            inner: keyspace,
            database: database.clone(),
        })
    }

    pub(crate) fn check_space_schema_and_insert_if_empty<K: Migrateable, V: Migrateable>(
        &self,
        key: &str,
    ) -> anyhow::Result<ValidatedSchema<K, V>> {
        let mut existing_key_schema = Vec::<(K, u128)>::new();
        let mut existing_value_schema = Vec::<(V, u128)>::new();

        let mut tx = self.database.write_tx()?;

        for kv in tx.iter_prefixed(&self.database, &self.inner, encode_key_prefix(&key)?) {
            let (existing_key, existing_value) = kv?;
            let existing_key: SpaceKey<SchemaKey> = existing_key.into();
            let existing_key = existing_key.get();

            let hash =
                u128::from_le_bytes(existing_value.as_ref().try_into().map_err(|e: TryFromSliceError| anyhow::anyhow!(e))?);

            match existing_key.schema_type {
                SchemaType::Key => {
                    let version = K::from_u8(existing_key.version).ok_or_else(|| {
                        anyhow::anyhow!("New migrateable schema missing key version {} for {key}", existing_key.version)
                    })?;

                    if existing_key.data_kind != version.data_kind() as u8 {
                        return Err(anyhow::anyhow!(
                            "Data kind changed from {:?} to {:?} for key version {} in {key}",
                            MigrateDataKind::from_u8(existing_key.data_kind)
                                .map(|k| format!("{:?}", k))
                                .unwrap_or("Unknown".to_string()),
                            version.data_kind(),
                            existing_key.version
                        ));
                    }

                    existing_key_schema.push((version, hash));
                }
                SchemaType::Value => {
                    let version = V::from_u8(existing_key.version).ok_or_else(|| {
                        anyhow::anyhow!("New migrateable schema missing value version {} for {key}", existing_key.version)
                    })?;

                    if existing_key.data_kind != version.data_kind() as u8 {
                        return Err(anyhow::anyhow!(
                            "Data kind changed from {:?} to {:?} for value version {} in {key}",
                            MigrateDataKind::from_u8(existing_key.data_kind)
                                .map(|k| format!("{:?}", k))
                                .unwrap_or("Unknown".to_string()),
                            version.data_kind(),
                            existing_key.version
                        ));
                    }

                    existing_value_schema.push((version, hash));
                }
            }
        }

        for (key_version, stored_hash) in &existing_key_schema {
            if key_version.type_hash() != *stored_hash {
                return Err(anyhow::anyhow!("Schema type hash conflict in {key} for key version {}", key_version.to_u8()));
            }
        }
        for (value_version, stored_hash) in &existing_value_schema {
            if value_version.type_hash() != *stored_hash {
                return Err(anyhow::anyhow!("Schema type hash conflict in {key} for value version {}", value_version.to_u8()));
            }
        }

        if existing_key_schema.is_empty() {
            for version in K::iter() {
                let key = SchemaKey {
                    key: Cow::Owned(key.to_string()),
                    schema_type: SchemaType::Key,
                    data_kind: version.data_kind() as u8,
                    version: version.to_u8(),
                };
                tx.insert(&self.inner, key.to_slice(), version.type_hash().to_le_bytes());
            }

            existing_key_schema = K::iter()
                .map(|v| {
                    let hash = v.type_hash();
                    (v, hash)
                })
                .collect();
        }

        if existing_value_schema.is_empty() {
            for version in V::iter() {
                let key = SchemaKey {
                    key: Cow::Owned(key.to_string()),
                    schema_type: SchemaType::Value,
                    data_kind: version.data_kind() as u8,
                    version: version.to_u8(),
                };
                tx.insert(&self.inner, key.to_slice(), version.type_hash().to_le_bytes());
            }

            existing_value_schema = V::iter()
                .map(|v| {
                    let hash = v.type_hash();
                    (v, hash)
                })
                .collect();
        }

        tx.commit()??;
        Ok(ValidatedSchema::new(SpaceSchema::new(existing_key_schema, existing_value_schema)))
    }

    /// Records `next` as the space's schema, but only when it differs from
    /// `previous`, so a reopen that changed nothing writes nothing.
    pub(crate) fn write_space_schema_if_changed<K: Migrateable, V: Migrateable>(
        &self,
        key: &str,
        previous: &SpaceSchema<K, V>,
        next: &SpaceSchema<K, V>,
    ) -> anyhow::Result<()> {
        let previous_key = previous.key_schema.iter().map(|(v, h)| (v.to_u8(), h)).collect::<Vec<_>>();
        let next_key = next.key_schema.iter().map(|(v, h)| (v.to_u8(), h)).collect::<Vec<_>>();
        let previous_value = previous.value_schema.iter().map(|(v, h)| (v.to_u8(), h)).collect::<Vec<_>>();
        let next_value = next.value_schema.iter().map(|(v, h)| (v.to_u8(), h)).collect::<Vec<_>>();

        if previous_key != next_key || previous_value != next_value {
            self.update_space_schema(key, next)?;
        }
        Ok(())
    }

    pub(crate) fn remove_space(&self, key: &str) -> anyhow::Result<()> {
        let mut tx = self.database.write_tx()?;
        for existing_key in
            tx.iter_keys_prefixed(&self.database, &self.inner, encode_key_prefix(&key)?).collect::<Result<Vec<_>, _>>()?
        {
            tx.remove(&self.inner, existing_key);
        }
        tx.commit()??;
        Ok(())
    }

    fn update_space_schema<K: Migrateable, V: Migrateable>(&self, key: &str, schema: &SpaceSchema<K, V>) -> anyhow::Result<()> {
        let mut tx = self.database.write_tx()?;

        let updated_key_schema = schema
            .key_schema
            .iter()
            .map(|(k, v)| {
                (
                    SchemaKey {
                        key: Cow::Owned(key.to_string()),
                        schema_type: SchemaType::Key,
                        data_kind: k.data_kind() as u8,
                        version: k.to_u8(),
                    }
                    .to_slice(),
                    v,
                )
            })
            .collect::<HashMap<Slice, &u128>>();
        let updated_value_schema = schema
            .value_schema
            .iter()
            .map(|(v, hash)| {
                (
                    SchemaKey {
                        key: Cow::Owned(key.to_string()),
                        schema_type: SchemaType::Value,
                        data_kind: v.data_kind() as u8,
                        version: v.to_u8(),
                    }
                    .to_slice(),
                    hash,
                )
            })
            .collect::<HashMap<Slice, &u128>>();

        for existing_key in
            tx.iter_keys_prefixed(&self.database, &self.inner, encode_key_prefix(&key)?).collect::<Result<Vec<_>, _>>()?
        {
            if let Some(updated_hash) = updated_key_schema.get(&existing_key) {
                tx.fetch_update(&self.inner, existing_key, |_| Some(updated_hash.to_le_bytes().into()))?;
                continue;
            }
            if let Some(updated_hash) = updated_value_schema.get(&existing_key) {
                tx.fetch_update(&self.inner, existing_key, |_| Some(updated_hash.to_le_bytes().into()))?;
                continue;
            }
            tx.remove(&self.inner, existing_key);
        }

        tx.commit()??;
        self.database.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}