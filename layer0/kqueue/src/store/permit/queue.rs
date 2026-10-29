use std::{convert, marker};

use dope_sys::table::permit;
use o3::depot;

/// Uninhabited receive authority with no registration storage.
/// Key and index domains stay invariant without owning either.
pub struct Queue<K, I>(marker::PhantomData<fn(&mut (K, I))>);

impl<K: permit::raw::Key, I: depot::raw::Compact> permit::Queue for Queue<K, I> {
    type Key = K;
    type Index = I;
    type Capability = convert::Infallible;

    fn try_acquire() -> Result<Self::Capability, permit::Absent> {
        Err(permit::Absent)
    }

    fn try_arm(&self, permit: &Self::Capability, _: I, _: K) -> Result<(), K> {
        match *permit {}
    }

    fn cancel(&self, permit: &Self::Capability, _: I, _: K) -> permit::State<K> {
        match *permit {}
    }

    fn take(&self, permit: &Self::Capability, _: I, _: K) -> permit::State<K> {
        match *permit {}
    }

    fn grant_one(&self, permit: &Self::Capability) -> Option<(I, K)> {
        match *permit {}
    }
}

impl<K: permit::raw::Key, I: depot::raw::Compact> permit::Empty for Queue<K, I> {
    fn new() -> Self {
        Self(marker::PhantomData)
    }
}
