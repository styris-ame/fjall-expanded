use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use fjall::Slice;

use crate::{KeySource, ToKeySlice, versioned_key_slice::ToVersionedKeySlice};

pub(crate) fn make_versioned_prefix<P: AsRef<[u8]>>(key_version: u8, value_version: u8, prefix: P) -> Vec<u8> {
    let mut result = Vec::with_capacity(prefix.as_ref().len() + 2);
    result.push(key_version);
    result.push(value_version);
    result.extend_from_slice(prefix.as_ref());
    result
}

pub struct VersionedKey<T: KeySource<T>>(Slice, PhantomData<T>);
impl<T> VersionedKey<T>
where
    T: KeySource<T>,
{
    pub(crate) fn new(value: Slice) -> Self {
        Self(value, PhantomData)
    }

    pub fn get<'a>(&'a self) -> T::GetType<'a> {
        T::decode(&self.0[2..])
    }
    pub unsafe fn get_slice(&self) -> &[u8] {
        &self.0[2..]
    }
    pub unsafe fn into_versioned_slice(self) -> Slice {
        self.0
    }
}

impl<T: KeySource<T>> Clone for VersionedKey<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<T: KeySource<T>> PartialEq for VersionedKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: KeySource<T>> Eq for VersionedKey<T> {}

impl<T: KeySource<T>> Hash for VersionedKey<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.hash(state);
    }
}

pub struct VersionedKeyRef<'a, T: KeySource<T>>(&'a Slice, PhantomData<T>);
impl<'a, T> VersionedKeyRef<'a, T>
where
    T: KeySource<T>,
{
    pub(crate) fn new(value: &'a Slice) -> Self {
        Self(value, PhantomData)
    }

    pub fn get(&'a self) -> T::GetType<'a> {
        T::decode(&self.0[2..])
    }
    pub unsafe fn get_slice(&self) -> &[u8] {
        &self.0[2..]
    }
    pub unsafe fn to_versioned_slice(self) -> Slice {
        self.0.clone()
    }
    pub fn to_owned(self) -> VersionedKey<T> {
        VersionedKey::new(self.0.clone())
    }
}

impl<T: KeySource<T>> Clone for VersionedKeyRef<'_, T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T: KeySource<T>> PartialEq for VersionedKeyRef<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: KeySource<T>> Eq for VersionedKeyRef<'_, T> {}

impl<T: KeySource<T>> Hash for VersionedKeyRef<'_, T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.hash(state);
    }
}

// Re-encode the 2-byte version prefix with the target space's versions; the body
// (`self.0[2..]`) is space-agnostic but the prefix is not, so a VersionedKey pulled
// from one space (e.g. an alias value) must not carry its source prefix into another.
fn reversion(slice: &Slice, key_version: u8, value_version: u8) -> Slice {
    // Already the right prefix: clone the refcounted Slice, no Vec allocation.
    if slice[0] == key_version && slice[1] == value_version {
        return slice.clone();
    }
    let body = &slice[2..];
    let mut buffer = Vec::with_capacity(2 + body.len());
    buffer.push(key_version);
    buffer.push(value_version);
    buffer.extend_from_slice(body);
    buffer.into()
}

impl<T: KeySource<T>> ToVersionedKeySlice<T> for VersionedKey<T> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        if self.0[0] == key_version && self.0[1] == value_version {
            self.0
        } else {
            reversion(&self.0, key_version, value_version)
        }
    }
}
impl<T: KeySource<T>> ToVersionedKeySlice<T> for VersionedKeyRef<'_, T> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        reversion(self.0, key_version, value_version)
    }
}
impl<T: KeySource<T>> ToVersionedKeySlice<T> for &VersionedKey<T> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        reversion(&self.0, key_version, value_version)
    }
}
impl<T: KeySource<T>> ToVersionedKeySlice<T> for &VersionedKeyRef<'_, T> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        reversion(self.0, key_version, value_version)
    }
}

impl<T: KeySource<T>> ToKeySlice<T> for VersionedKey<T> {
    fn to_slice(self) -> Slice {
        unsafe { self.get_slice().into() }
    }
}
impl<T: KeySource<T>> ToKeySlice<T> for VersionedKeyRef<'_, T> {
    fn to_slice(self) -> Slice {
        unsafe { self.get_slice().into() }
    }
}
impl<T: KeySource<T>> ToKeySlice<T> for &VersionedKey<T> {
    fn to_slice(self) -> Slice {
        unsafe { self.get_slice().into() }
    }
}
impl<T: KeySource<T>> ToKeySlice<T> for &VersionedKeyRef<'_, T> {
    fn to_slice(self) -> Slice {
        unsafe { self.get_slice().into() }
    }
}
