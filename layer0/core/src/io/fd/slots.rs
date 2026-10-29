use std::mem;

use crate::io::fd::{domain, handles, identity};

#[must_use = "a descriptor reservation must be installed or returned"]
#[repr(transparent)]
pub(crate) struct ReservedSlot<'d> {
    solo: domain::Solo<identity::Identity<'d>>,
}

impl<'d> ReservedSlot<'d> {
    pub(crate) const fn bind(solo: domain::Solo<identity::Identity<'d>>) -> Self {
        Self { solo }
    }

    pub(crate) fn physical(&self) -> handles::PhysicalSlot {
        self.solo.physical()
    }

    pub(crate) const fn into_inner(self) -> domain::Solo<identity::Identity<'d>> {
        self.solo
    }
}

const _: () =
    assert!(mem::size_of::<ReservedSlot<'_>>() == mem::size_of::<identity::Identity<'_>>());
