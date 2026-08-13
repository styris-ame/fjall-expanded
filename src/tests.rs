use std::{borrow::Cow, marker::PhantomData};

use crate::{Key, SpaceKey, ToKeySlice};
use fjall::Slice;
use storekey::{BorrowDecode, Encode};

pub struct Test<T: storekey::Encode + Key> {
    _marker: PhantomData<T>,
}

impl<T> Test<T>
where
    T: storekey::Encode + Key,
{
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}
impl<T> Test<T>
where
    T: storekey::Encode + Key,
{
    pub fn take_and_return_key(&self, key: impl ToKeySlice<T>) -> SpaceKey<T> {
        let key_slice = key.to_slice();
        key_slice.into()
    }
}

#[derive(storekey::Encode, storekey::BorrowDecode, SpaceKey)]
struct TestSpaceKey<'a> {
    version: u8,
    uuid: [u8; 16],
    path: Cow<'a, str>,
}
#[derive(storekey::Encode, storekey::BorrowDecode, SpaceKey)]
struct TestSpaceKeyOwned {
    version: u8,
    uuid: [u8; 16],
    path: String,
}

#[test]
fn test_storekey() {
    let database = Test::<TestSpaceKey>::new();

    let t = TestSpaceKey {
        version: 1,
        uuid: [0; 16],
        path: Cow::Owned("test/path".to_string()),
    };

    let key = database.take_and_return_key(t);
    let key = database.take_and_return_key(key);
    let _decoded = key.get();
}

#[test]
fn test_storekey_owned() {
    let database = Test::<TestSpaceKeyOwned>::new();

    let t = TestSpaceKeyOwned {
        version: 1,
        uuid: [0; 16],
        path: "test/path".to_string(),
    };

    let key = database.take_and_return_key(t);
    let key = database.take_and_return_key(key);
    let _decoded = key.get();
}
