use std::mem;

use dope_sys::{io::reactor::complete, kernel, probe::receive::Partial, table};
use o3::{base::num::bounded, buffer::storage::provided::fixed};

use crate::{
    Engine,
    driver::{
        receive,
        result::poll,
        retained::flight,
        route,
        route::{indices, kind, table::Capacity},
    },
    io::{fd::handles, transfer},
};

/// Exact stale arena echo retained after its owning flight disappeared.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Echo(flight::raw::Echo);

impl Echo {
    pub(crate) const fn new(echo: flight::raw::Echo) -> Self {
        Self(echo)
    }
}

/// Exact typed kernel completion observed at the reactor boundary.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Observation(<Engine as complete::Observe>::Observation);

/// Number of submission entries left in a ring during a probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Pending(bounded::U32<0, { u32::MAX }>);

/// Number of completion entries left in a ring during a probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Available(bounded::U32<0, { u32::MAX }>);

impl From<bounded::U32<0, { u32::MAX }>> for Pending {
    fn from(value: bounded::U32<0, { u32::MAX }>) -> Self {
        Self(value)
    }
}

impl Pending {
    pub const fn is_zero(self) -> bool {
        self.0.get() == 0
    }
}

impl From<bounded::U32<0, { u32::MAX }>> for Available {
    fn from(value: bounded::U32<0, { u32::MAX }>) -> Self {
        Self(value)
    }
}

impl Available {
    pub const fn is_zero(self) -> bool {
        self.0.get() == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Send {
    actual: usize,
    expected: usize,
}

impl Send {
    pub(crate) fn from_partial(partial: Partial) -> Self {
        let (actual, expected) = partial.into();
        Self { actual, expected }
    }
}

impl Observation {
    pub(crate) const fn new(observed: <Engine as complete::Observe>::Observation) -> Self {
        Self(observed)
    }
}

/// Exact malformed raw completion data retained at its decode boundary.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Raw {
    BufferIndex(receive::Selection),
    Flags,
}

/// Semantic contract rejected while decoding one completion.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Violation {
    Raw(Raw),
    Stale(Echo),
    UserData,
    Control,
    Buffer,
    Provided(fixed::Full),
    BufferLength {
        actual: transfer::Len,
        capacity: transfer::NonZero,
    },
    Result,
    /// Exact native completion that violated an operation contract.
    Outcome(<crate::Backend as kernel::Source>::Returned),
    Slot,
    SlotIndex(indices::SlotOverflow),
    Install(Install),
    Descriptor((indices::SlotIndex, Capacity)),
    Operation {
        expected: kind::Native,
        observed: kind::Native,
    },
    Tuning(kind::Native),
    Close,
    Class {
        expected: handles::Retirement,
        observed: handles::Retirement,
    },
    RawClass {
        cause: Raw,
        expected: handles::Retirement,
        observed: handles::Retirement,
    },
    Cleanup(crate::Errno),
}

/// Exact semantic identity decoded from a completion, when one exists.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Subject {
    Unbound,
    Target(route::Token),
    Control(route::Control),
    CancelFlight(Echo),
    CancelTuning(indices::SlotIndex),
    Tuning { echo: Echo, control: route::Control },
}

impl From<route::Erased<'_, kind::Any>> for Subject {
    fn from(target: route::Erased<'_, kind::Any>) -> Self {
        Self::Target(target.into_token())
    }
}

/// Lossless, allocation-free completion failure returned to the driver caller.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct Fault {
    pub observation: Observation,
    pub subject: Subject,
    pub violation: Violation,
}

/// Completion fault paired with the exact source-drain state after repair.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Dispatch {
    pub drain: poll::Drain,
    pub fault: Fault,
}

impl Dispatch {
    pub(crate) const fn new(drain: poll::Drain, fault: Fault) -> Self {
        Self { drain, fault }
    }
}

const _: () = {
    assert!(mem::size_of::<Echo>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Pending>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Pending>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Available>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Available>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Raw>() == mem::size_of::<(u8, receive::Selection)>());
    assert!(mem::align_of::<Raw>() == mem::align_of::<(u8, receive::Selection)>());
    assert!(mem::size_of::<Subject>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Fault>() == mem::size_of::<Observation>() + 5 * mem::size_of::<usize>());
    assert!(mem::align_of::<Fault>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Dispatch>() == mem::size_of::<Fault>() + mem::size_of::<usize>());
};

/// Exact descriptor-table occupancy rejected during completion installation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Install {
    Index(indices::SlotIndex),
    Occupied(indices::SlotIndex),
}

impl Install {
    pub(crate) const fn from_native(error: table::Install<indices::SlotIndex>) -> Self {
        use dope_sys::table::Install;
        match error {
            Install::Index(index) => Self::Index(index),
            Install::Occupied(index) => Self::Occupied(index),
        }
    }
}
