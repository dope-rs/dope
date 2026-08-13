use std::mem;

pub mod egress;
mod engine;
pub mod event;
pub mod pool;
mod raw;
mod sealed;
mod setup;
pub mod slot;

pub(in crate::link) use engine::AcceptedTuning;
pub use engine::Engine;
pub(crate) use sealed::{Connect, Rearm, Receive};
pub(in crate::link) use setup::{Authority, Setup};

/// Bytes proven to fit in the exact plain view handed to the wire.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Consumed(usize);

const _: () = assert!(mem::size_of::<Consumed>() == mem::size_of::<usize>());

impl Consumed {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> usize {
        self.0
    }

    pub(crate) const fn proven(amount: usize) -> Self {
        Self(amount)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EgressError;
