use std::{cell::Cell, num::NonZero};

use dope::core::driver::schedule::ready::task::owner;
use dope_fiber::task::storage::set::group;
use o3::depot::map;

pub fn owners() -> owner::Capacity {
    owner::Capacity::new(NonZero::<u32>::MIN.saturating_add(255))
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Key(pub u8);

impl owner::Identity for Key {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (
            owner::Lane::from(NonZero::<u32>::MIN.saturating_add(u32::from(self.0))),
            owner::Generation::from_u32(0),
        )
    }
}

pub struct First;

impl group::Policy<(u8, u8)> for First {
    type Key = Key;

    fn key(task: &(u8, u8)) -> Key {
        Key(task.0)
    }
}

impl map::Hasher<Key> for First {
    fn hash(&self, key: &Key) -> u64 {
        u64::from(key.0 / 3)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChangingKey<'a> {
    value: u8,
    mode: &'a Cell<u8>,
}

impl<'a> ChangingKey<'a> {
    pub fn new(value: u8, mode: &'a Cell<u8>) -> Self {
        Self { value, mode }
    }
}

impl PartialEq for ChangingKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.mode.get() == 1 || self.value == other.value
    }
}

impl owner::Identity for ChangingKey<'_> {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        Key(self.value).parts()
    }
}

impl Eq for ChangingKey<'_> {}

pub struct Changing<'a>(pub &'a Cell<u8>);

impl<'a> group::Policy<ChangingKey<'a>> for Changing<'a> {
    type Key = ChangingKey<'a>;

    fn key(task: &Self::Key) -> Self::Key {
        *task
    }
}

impl map::Hasher<ChangingKey<'_>> for Changing<'_> {
    fn hash(&self, _: &ChangingKey<'_>) -> u64 {
        u64::from(self.0.get() == 2)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EpochKey(pub u8, pub u32);

impl owner::Identity for EpochKey {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (
            owner::Lane::from(NonZero::<u32>::MIN.saturating_add(u32::from(self.0))),
            owner::Generation::from_u32(self.1),
        )
    }
}

pub struct Epochs;

impl group::Policy<EpochKey> for Epochs {
    type Key = EpochKey;
    fn key(task: &EpochKey) -> EpochKey {
        *task
    }
}

impl map::Hasher<EpochKey> for Epochs {
    fn hash(&self, key: &EpochKey) -> u64 {
        u64::from(key.0)
    }
}
