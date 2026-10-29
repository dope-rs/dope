//! Process configuration failures.

use std::mem;

use dope_core::platform::affinity;

use crate::executor::drive::clock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
/// Invalid process topology or supervision timing.
pub enum Error {
    Empty,
    DuplicateCpu(affinity::Cpu),
    ResourceCount { actual: usize, required: usize },
    ResourceIterator { yielded: usize, required: usize },
    HeartbeatDeadline(clock::Range),
}

const _: () = {
    assert!(!mem::needs_drop::<Error>());
    assert!(mem::size_of::<Error>() == mem::size_of::<clock::Range>() + mem::align_of::<Error>());
};
