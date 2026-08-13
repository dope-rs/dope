//! Fixed ready-generation identity.

use std::mem;

use crate::driver::schedule::ready;

/// Non-operational identity of one fixed ready generation.
#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
#[repr(transparent)]
pub struct FixedIdentity<'d>(ready::FixedKey<'d>);

impl<'d> FixedIdentity<'d> {
    pub(in crate::driver::schedule::ready) const fn new(key: ready::FixedKey<'d>) -> Self {
        Self(key)
    }
}

const _: () =
    assert!(mem::size_of::<FixedIdentity<'static>>() == mem::size_of::<ready::Key<'static>>());
