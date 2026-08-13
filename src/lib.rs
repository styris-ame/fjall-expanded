use std::marker::PhantomData;

use anyhow::Result;
use easy_rkyv::TrustedRkyv;
use fjall::{Guard, Slice};
#[cfg(feature = "derive")]
pub use fjall_expanded_derive::SpaceKey;

pub mod alias_space;
pub mod key_slice;
#[cfg(feature = "migrateable")]
pub mod large_blob_space;
pub mod rkyv_space;
pub mod schema_map_space;
#[cfg(feature = "search")]
pub mod search;
pub mod store_key;
pub mod store_space;
pub mod managed_database;
pub mod versioned_alias_space;
pub mod versioned_key;
pub mod versioned_key_slice;
pub mod versioned_rkyv_space;
pub mod versioned_store_space;

pub use alias_space::AliasSpace;
pub use key_slice::ToKeySlice;
#[cfg(feature = "migrateable")]
pub use large_blob_space::{LargeBlobReader, LargeBlobSpace, LargeBlobWriter, StagedBlob};
use rkyv::{
    Archive, Deserialize, Portable,
    api::high::{HighDeserializer, HighValidator},
    bytecheck::CheckBytes,
    rancor,
};
pub use rkyv_space::RkyvSpace;
pub use schema_map_space::SchemaMapSpace;
#[cfg(feature = "search")]
pub use search::{
    DEFAULT_MIN_TOKEN_LENGTH, ParsedQuery, SearchSpace, VersionedSearchSpace, WordSpan, parse_query, tokenize, tokenize_spans,
};
pub use store_key::{SpaceKey, SpaceKeyRef};
pub use store_space::StoreSpace;
pub use managed_database::{
    BACKUP_SUFFIX, BackupEntry, BackupProgress, BackupSettings, BlobOptions, DatabaseInitializer, SpaceGroup, SpaceOptions,
    ManagedDatabase, ManagedDatabaseConfig, is_backup_archive, list_backups, namespaced, open_managed_database,
    pack_prerestore_snapshot,
};
pub use versioned_alias_space::VersionedAliasSpace;
pub use versioned_key::{VersionedKey, VersionedKeyRef};
pub use versioned_rkyv_space::VersionedRkyvSpace;
pub use versioned_store_space::VersionedStoreSpace;

pub mod optimistic_tx;
pub use optimistic_tx::*;

pub trait Key {
    type Borrowed<'a>: storekey::BorrowDecode<'a>;
}

pub(crate) const WRITE_BUFFER_CAP: u64 = 1024 * 1024 * 48;

/// Blocks until the write buffer drops below [`WRITE_BUFFER_CAP`]. The buffer counts
/// active memtables too, and those only get flushed once something rotates them, so a
/// buffer sitting entirely in active memtables would never drain on its own. If we've
/// been waiting with no flushes in flight, rotate everything to unstick it.
pub(crate) fn wait_for_write_buffer(database: &fjall::Database) {
    let mut idle_ticks = 0u32;
    let mut last_buffer_size = database.write_buffer_size();
    let mut lowest_buffer_size = last_buffer_size;
    while last_buffer_size > WRITE_BUFFER_CAP {
        let buffer_size = database.write_buffer_size();

        if database.outstanding_flushes() == 0 && buffer_size >= lowest_buffer_size {
            idle_ticks += 1;
            if idle_ticks >= 10 {
                idle_ticks = 0;
                for name in database.list_keyspace_names() {
                    if let Ok(keyspace) = database.keyspace(&name, fjall::KeyspaceCreateOptions::default) {
                        let _ = keyspace.rotate_memtable();
                    }
                }
            }
        } else {
            idle_ticks = 0;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));

        if buffer_size < lowest_buffer_size {
            lowest_buffer_size = buffer_size;
        }
        last_buffer_size = buffer_size;
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod stress_tests;

#[derive(Default)]
pub enum ReturnType {
    #[default]
    OldValue,
    NewValue,
}

pub(crate) mod key_source {
    use crate::Key;

    pub trait SealedKeySource<T> {}

    impl<T: storekey::Encode + Key> SealedKeySource<T> for T {}
    impl<T: storekey::Encode + Key> SealedKeySource<T> for &T {}
    impl<const N: usize> SealedKeySource<[u8; N]> for [u8; N] {}
    impl<const N: usize> SealedKeySource<[u8; N]> for &[u8; N] {}
    impl SealedKeySource<String> for String {}
    impl SealedKeySource<String> for &str {}
}

pub trait KeySource<T>: key_source::SealedKeySource<T> {
    type GetType<'a>;

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a>;
}

impl<const N: usize> KeySource<[u8; N]> for [u8; N] {
    type GetType<'a> = [u8; N];

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        slice.as_ref().try_into().expect("Slice length mismatch")
    }
}
impl<const N: usize> KeySource<[u8; N]> for &[u8; N] {
    type GetType<'a> = [u8; N];

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        slice.as_ref().try_into().expect("Slice length mismatch")
    }
}
impl KeySource<String> for String {
    type GetType<'a> = String;

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        String::from_utf8(slice.to_vec()).expect("Invalid UTF-8")
    }
}
impl KeySource<String> for &str {
    type GetType<'a> = String;

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        String::from_utf8(slice.to_vec()).expect("Invalid UTF-8")
    }
}
impl<T: storekey::Encode + crate::Key> KeySource<T> for T {
    type GetType<'a> = T::Borrowed<'a>;

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        storekey::decode_borrow::<'a, Self::GetType<'a>>(slice).expect("Key decoding failed")
    }
}
impl<T: storekey::Encode + crate::Key> KeySource<T> for &T {
    type GetType<'a> = T::Borrowed<'a>;

    fn decode<'a>(slice: &'a [u8]) -> Self::GetType<'a> {
        storekey::decode_borrow::<'a, Self::GetType<'a>>(slice).expect("Key decoding failed")
    }
}

pub struct DataGuard<K, V> {
    inner: Guard,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> DataGuard<K, V>
where
    K: KeySource<K>,
    V: TryFrom<Slice>,
    <V as TryFrom<Slice>>::Error: Into<anyhow::Error>,
{
    fn new(guard: Guard) -> Self {
        Self {
            inner: guard,
            _marker: PhantomData,
        }
    }

    pub fn key(self) -> fjall::Result<SpaceKey<K>> {
        self.inner.key().map(Into::into)
    }

    pub fn value(self) -> Result<V> {
        let raw = self.inner.value()?;
        V::try_from(raw).map_err(Into::into)
    }

    pub fn into_inner(self) -> Result<(SpaceKey<K>, V)> {
        let (key, value) = self.inner.into_inner()?;
        let typed_value = V::try_from(value).map_err(Into::into)?;
        Ok((key.into(), typed_value))
    }

    pub fn raw(&self) -> &Guard {
        &self.inner
    }

    pub fn into_raw(self) -> Guard {
        self.inner
    }
}

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

    pub fn key(self) -> fjall::Result<SpaceKey<K>> {
        self.inner.key().map(Into::into)
    }

    pub fn value(self) -> fjall::Result<TrustedRkyv<V>> {
        let raw = self.inner.value()?;
        Ok(TrustedRkyv::new(raw))
    }

    pub fn into_inner(self) -> fjall::Result<(SpaceKey<K>, TrustedRkyv<V>)> {
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

pub fn encode_key_prefix<E: storekey::Encode + ?Sized>(e: &E) -> Result<Vec<u8>> {
    storekey::encode_vec(e).map_err(anyhow::Error::from_boxed)
}

pub struct AliasGuard<K, V> {
    inner: Guard,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> AliasGuard<K, V>
where
    K: KeySource<K>,
    V: KeySource<V>,
{
    fn new(guard: Guard) -> Self {
        Self {
            inner: guard,
            _marker: PhantomData,
        }
    }

    pub fn key(self) -> fjall::Result<SpaceKey<K>> {
        self.inner.key().map(Into::into)
    }

    pub fn value(self) -> Result<SpaceKey<V>> {
        let raw = self.inner.value()?;
        Ok(raw.into())
    }

    pub fn into_inner(self) -> Result<(SpaceKey<K>, SpaceKey<V>)> {
        let (key, value) = self.inner.into_inner()?;
        Ok((key.into(), value.into()))
    }

    pub unsafe fn raw(&self) -> &Guard {
        &self.inner
    }

    pub unsafe fn into_raw(self) -> Guard {
        self.inner
    }
}
