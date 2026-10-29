use std::{marker, mem};

use dope_core::driver::route::{Identity, KeyTag, indices};

use o3::base::cell::region;

/// Generation-free physical connector lane, branded by driver, route, and
/// connection family. Exact-generation authority remains in [`super::Id`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Lane<'d, ID: Identity> {
    slot: indices::SlotIndex,
    brand: marker::PhantomData<fn(region::Identity<'d>) -> ID>,
}

impl<ID: Identity> Lane<'_, ID> {
    pub(super) const fn new(slot: indices::SlotIndex) -> Self {
        Self {
            slot,
            brand: marker::PhantomData,
        }
    }

    /// Returns the exact physical route slot.
    pub const fn slot(self) -> indices::SlotIndex {
        self.slot
    }
}

const _: () =
    assert!(mem::size_of::<Lane<'static, KeyTag<0>>>() == mem::size_of::<indices::SlotIndex>());
const _: () =
    assert!(mem::align_of::<Lane<'static, KeyTag<0>>>() == mem::align_of::<indices::SlotIndex>());
