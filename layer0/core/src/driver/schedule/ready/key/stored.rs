use std::num;

use dope_sys::table::permit;
use o3::base::marker::ThreadBound;

use crate::driver::schedule::{ready, ready::key::DynamicIndex};

/// Lifetime-free exact key retained only by a driver-owned adapter.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Stored(Value, ThreadBound);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C, align(8))]
struct Value {
    index: num::NonZero<u16>,
    epoch: ready::Generation,
}

impl Stored {
    pub(in crate::driver) const fn from_parts(
        index: num::NonZero<u16>,
        epoch: ready::Generation,
    ) -> Self {
        Self(Value { index, epoch }, ThreadBound::NEW)
    }

    pub(in crate::driver::schedule::ready) const fn index_code(self) -> num::NonZero<u16> {
        self.0.index
    }

    pub(in crate::driver) fn index(self) -> u32 {
        u32::from(self.index_code().get() - 1)
    }

    pub(in crate::driver) const fn dense(self) -> DynamicIndex {
        DynamicIndex(self.0.index)
    }

    pub(in crate::driver) fn epoch(self) -> ready::Generation {
        self.0.epoch
    }
}

// SAFETY: the nonzero scalar is the complete generation and index; extraction
// is total and invokes no user code.
unsafe impl permit::raw::Key for Stored {
    fn identity(self) -> u64 {
        let value = self.0;
        ((value.epoch.get() as u64) << 32) | value.index.get() as u64
    }
}
