use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use fjall::Slice;

use crate::{KeySource, ToKeySlice};

pub struct SpaceKey<T: KeySource<T>>(Slice, PhantomData<T>);
impl<T> SpaceKey<T>
where
    T: KeySource<T>,
{
    pub(crate) fn new(value: Slice) -> Self {
        Self(value, PhantomData)
    }

    pub fn get<'a>(&'a self) -> T::GetType<'a> {
        T::decode(&self.0)
    }
    pub unsafe fn get_slice(&self) -> &Slice {
        &self.0
    }
    pub unsafe fn into_slice(self) -> Slice {
        self.0
    }
}

impl<T: KeySource<T>> Clone for SpaceKey<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<T: KeySource<T>> PartialEq for SpaceKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: KeySource<T>> Eq for SpaceKey<T> {}

impl<T: KeySource<T>> Hash for SpaceKey<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.hash(state);
    }
}

pub struct SpaceKeyRef<'a, T: KeySource<T>>(&'a Slice, PhantomData<T>);
impl<'a, T> SpaceKeyRef<'a, T>
where
    T: KeySource<T>,
{
    pub(crate) fn new(value: &'a Slice) -> Self {
        Self(value, PhantomData)
    }

    pub fn get(&'a self) -> T::GetType<'a> {
        T::decode(&self.0)
    }
    pub unsafe fn get_slice(&self) -> &Slice {
        &self.0
    }
    pub unsafe fn to_slice(self) -> Slice {
        self.0.clone()
    }
    pub fn to_owned(self) -> SpaceKey<T> {
        SpaceKey::new(self.0.clone())
    }
}

impl<T: KeySource<T>> Clone for SpaceKeyRef<'_, T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T: KeySource<T>> PartialEq for SpaceKeyRef<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: KeySource<T>> Eq for SpaceKeyRef<'_, T> {}

impl<T: KeySource<T>> Hash for SpaceKeyRef<'_, T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.hash(state);
    }
}

impl<T: KeySource<T>> ToKeySlice<T> for SpaceKey<T> {
    fn to_slice(self) -> Slice {
        self.0
    }
}

impl<T: KeySource<T>> ToKeySlice<T> for SpaceKeyRef<'_, T> {
    fn to_slice(self) -> Slice {
        self.0.clone()
    }
}

impl<T: KeySource<T>> ToKeySlice<T> for &SpaceKey<T> {
    fn to_slice(self) -> Slice {
        self.0.clone()
    }
}
impl<T: KeySource<T>> ToKeySlice<T> for &SpaceKeyRef<'_, T> {
    fn to_slice(self) -> Slice {
        self.0.clone()
    }
}
