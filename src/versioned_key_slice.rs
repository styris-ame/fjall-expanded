use std::borrow::Cow;

use fjall::Slice;
use rkyv::string::ArchivedString;
use storekey::{EncodeError, Writer};

use crate::{Key, KeySource};

pub(crate) mod versioned_key_slice {
    use std::borrow::Cow;

    use fjall::Slice;
    use rkyv::string::ArchivedString;

    use crate::{Key, KeySource, VersionedKey, versioned_key::VersionedKeyRef};

    impl<T: KeySource<T>> From<Slice> for VersionedKey<T> {
        fn from(key: Slice) -> Self {
            VersionedKey::new(key)
        }
    }
    impl<'a, T: KeySource<T>> From<&'a Slice> for VersionedKeyRef<'a, T> {
        fn from(slice: &'a Slice) -> Self {
            VersionedKeyRef::new(slice)
        }
    }

    pub trait SealedToVersionedKeySlice<T: KeySource<T>> {}
    impl<T: KeySource<T>> SealedToVersionedKeySlice<T> for VersionedKeyRef<'_, T> {}
    impl<T: KeySource<T>> SealedToVersionedKeySlice<T> for VersionedKey<T> {}
    impl<T: KeySource<T>> SealedToVersionedKeySlice<T> for &VersionedKey<T> {}
    impl<T: KeySource<T>> SealedToVersionedKeySlice<T> for &VersionedKeyRef<'_, T> {}

    impl<T: storekey::Encode + Key> SealedToVersionedKeySlice<T> for T {}
    impl<T: storekey::Encode + Key> SealedToVersionedKeySlice<T> for &T {}
    impl SealedToVersionedKeySlice<String> for &str {}
    impl SealedToVersionedKeySlice<String> for String {}
    impl SealedToVersionedKeySlice<String> for &String {}
    impl SealedToVersionedKeySlice<String> for ArchivedString {}
    impl SealedToVersionedKeySlice<String> for &ArchivedString {}
    impl SealedToVersionedKeySlice<String> for Cow<'_, str> {}
    impl<const N: usize> SealedToVersionedKeySlice<[u8; N]> for [u8; N] {}
    impl<const N: usize> SealedToVersionedKeySlice<[u8; N]> for &[u8; N] {}
}
pub trait ToVersionedKeySlice<T: KeySource<T>>: versioned_key_slice::SealedToVersionedKeySlice<T> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice;
}

impl<const N: usize> ToVersionedKeySlice<[u8; N]> for [u8; N] {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + N);
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(&self);
        buffer.into()
    }
}
impl<const N: usize> ToVersionedKeySlice<[u8; N]> for &[u8; N] {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + N);
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self);
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for String {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for &str {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for &String {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for ArchivedString {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for &ArchivedString {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}
impl ToVersionedKeySlice<String> for Cow<'_, str> {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        let mut buffer = Vec::with_capacity(2 + self.len());
        buffer.push(key_version);
        buffer.push(value_version);
        buffer.extend_from_slice(self.as_bytes());
        buffer.into()
    }
}

impl<T: storekey::Encode + Key> ToVersionedKeySlice<T> for T {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        encode_versioned_key(&self, key_version, value_version)
    }
}

impl<T: storekey::Encode + Key> ToVersionedKeySlice<T> for &T {
    fn to_slice(self, key_version: u8, value_version: u8) -> Slice {
        encode_versioned_key(self, key_version, value_version)
    }
}

fn encode_versioned_key<T: storekey::Encode + Key>(value: &T, key_version: u8, value_version: u8) -> Slice {
    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer);
    writer.write_u8(key_version).expect("Key encoding failed");
    writer.write_u8(value_version).expect("Value encoding failed");
    match value.encode(&mut writer) {
        Ok(_) => Ok(buffer),
        // Encoding should only fail on a custom error or an io error, but as this is encoded to
        // vector it should not be able to fail.
        Err(EncodeError::Io(_)) => unreachable!(),
        Err(EncodeError::Custom(x)) => Err(x),
    }
    .expect("Key encoding failed")
    .into()
}
