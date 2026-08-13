use fjall::Slice;
use rkyv::string::ArchivedString;

use crate::KeySource;

pub(crate) mod key_slice {
    use fjall::Slice;
    use rkyv::string::ArchivedString;

    use crate::{Key, KeySource, SpaceKey, VersionedKey, VersionedKeyRef, store_key::SpaceKeyRef};

    impl<T: KeySource<T>> From<Slice> for SpaceKey<T> {
        fn from(key: Slice) -> Self {
            SpaceKey::new(key)
        }
    }
    impl<'a, T: KeySource<T>> From<&'a Slice> for SpaceKeyRef<'a, T> {
        fn from(key: &'a Slice) -> Self {
            SpaceKeyRef::new(key)
        }
    }

    pub trait SealedToKeySlice<T: KeySource<T>> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for SpaceKeyRef<'_, T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for SpaceKey<T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for &SpaceKeyRef<'_, T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for &SpaceKey<T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for VersionedKey<T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for VersionedKeyRef<'_, T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for &VersionedKey<T> {}
    impl<T: KeySource<T>> SealedToKeySlice<T> for &VersionedKeyRef<'_, T> {}
    impl<T: storekey::Encode + Key> SealedToKeySlice<T> for T {}
    impl<T: storekey::Encode + Key> SealedToKeySlice<T> for &T {}
    impl SealedToKeySlice<String> for &str {}
    impl SealedToKeySlice<String> for String {}
    impl SealedToKeySlice<String> for &String {}
    impl SealedToKeySlice<String> for ArchivedString {}
    impl SealedToKeySlice<String> for &ArchivedString {}
    impl SealedToKeySlice<String> for std::borrow::Cow<'_, str> {}
    impl<const N: usize> SealedToKeySlice<[u8; N]> for [u8; N] {}
    impl<const N: usize> SealedToKeySlice<[u8; N]> for &[u8; N] {}
}

pub trait ToKeySlice<T: KeySource<T>>: key_slice::SealedToKeySlice<T> {
    fn to_slice(self) -> Slice;
}

impl<const N: usize> ToKeySlice<[u8; N]> for [u8; N] {
    fn to_slice(self) -> Slice {
        self.into()
    }
}
impl<const N: usize> ToKeySlice<[u8; N]> for &[u8; N] {
    fn to_slice(self) -> Slice {
        self.into()
    }
}
impl ToKeySlice<String> for String {
    fn to_slice(self) -> Slice {
        self.into()
    }
}
impl ToKeySlice<String> for &str {
    fn to_slice(self) -> Slice {
        self.into()
    }
}
impl ToKeySlice<String> for &String {
    fn to_slice(self) -> Slice {
        self.into()
    }
}
impl ToKeySlice<String> for ArchivedString {
    fn to_slice(self) -> Slice {
        self.as_str().into()
    }
}
impl ToKeySlice<String> for &ArchivedString {
    fn to_slice(self) -> Slice {
        self.as_str().into()
    }
}
impl ToKeySlice<String> for std::borrow::Cow<'_, str> {
    fn to_slice(self) -> Slice {
        self.as_ref().into()
    }
}

impl<T: storekey::Encode + crate::Key> ToKeySlice<T> for T {
    fn to_slice(self) -> Slice {
        let encoded = storekey::encode_vec(&self).expect("Key encoding failed");
        encoded.into()
    }
}
impl<T: storekey::Encode + crate::Key> ToKeySlice<T> for &T {
    fn to_slice(self) -> Slice {
        let encoded = storekey::encode_vec(self).expect("Key encoding failed");
        encoded.into()
    }
}
