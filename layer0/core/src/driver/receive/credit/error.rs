use std::num;

use crate::driver::route::indices;

/// An index outside the active group's exact extent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Index {
    pub requested: indices::SlotIndex,
    pub capacity: num::NonZero<u32>,
}

/// Current attachment of the observed group generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Generation {
    Active(num::NonZero<u64>),
    Retired(num::NonZero<u64>),
}

/// A key whose generation no longer has active storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stale {
    pub expected: num::NonZero<u64>,
    pub observed: Generation,
}

/// Failure to arm an existing generation-bound key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arm {
    Stale(Stale),
    Busy,
    Detached,
}

const _: () = {
    assert!(size_of::<Index>() == 2 * size_of::<u32>());
    assert!(size_of::<Option<Index>>() == size_of::<Index>());
};
