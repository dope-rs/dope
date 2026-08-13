use std::{fmt, hash, mem};

use dope_core::driver::route::{self, table};

/// One generation-checked connection identity bound to its driver lifetime and route.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Key<'d, const ID: u8> {
    target: route::Target<'d, route::KeyTag<ID>>,
}

const _: () = assert!(mem::size_of::<Key<'static, 0>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Key<'static, 0>>() == mem::align_of::<route::Token>());

impl<const ID: u8> PartialEq for Key<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl<const ID: u8> Eq for Key<'_, ID> {}

impl<const ID: u8> hash::Hash for Key<'_, ID> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.target.hash(state);
    }
}

impl<const ID: u8> fmt::Debug for Key<'_, ID> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionKey")
            .field("route", &ID)
            .field("slot", &self.lane())
            .field("epoch", &self.epoch())
            .finish()
    }
}

impl<const ID: u8> From<Key<'_, ID>> for route::Token {
    fn from(key: Key<'_, ID>) -> Self {
        Self::from_target(key.target)
    }
}

impl<'d, const ID: u8> Key<'d, ID> {
    pub(in crate::link::pool) const fn from_target(
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> Self {
        Self { target }
    }

    pub(in crate::link) const fn target(self) -> route::Target<'d, route::KeyTag<ID>> {
        self.target
    }

    pub(in crate::link) const fn parts(self) -> table::Parts<route::KeyTag<ID>> {
        self.target.parts()
    }

    pub const fn lane(self) -> route::SlotIndex {
        self.target.slot()
    }

    pub const fn index(self) -> usize {
        self.lane().raw() as usize
    }

    pub const fn epoch(self) -> route::Epoch {
        self.target.epoch()
    }
}
