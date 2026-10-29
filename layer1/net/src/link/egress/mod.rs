use dope_core::io::transfer;
use o3::{
    buffer::{bytes::bounded, storage::Zero},
    mem::credit::fair,
};

pub mod create;
pub mod data;
pub mod load;
pub mod metadata;
mod queue;
pub mod reserve;
mod resident;
mod storage;

pub(in crate::link) use queue::{Change, Preparation, Reservation, flight::Output};
pub use queue::{
    Queue,
    batch::Batch,
    load::{Load, charge::Charge, empty::Empty, nonempty::NonEmpty, rejected::Rejected},
};
pub use resident::Resident;
pub(in crate::link) use storage::Storage;
pub use storage::config::Config;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct WireBytes(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct ResidentSize(usize);

impl WireBytes {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> usize {
        self.0
    }

    pub(in crate::link::egress) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    pub fn try_total(self, prefix: transfer::Len) -> Result<Option<transfer::NonZero>, WireError> {
        let body = transfer::Len::try_new(self.0).map_err(WireError::Body)?;
        prefix
            .checked_add(body)
            .map(transfer::Len::nonzero)
            .map_err(WireError::Total)
    }
}

impl ResidentSize {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> usize {
        self.0
    }

    pub(in crate::link::egress) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    pub(in crate::link::egress) const fn is_excess(self, maximum: usize) -> bool {
        self.0 > maximum
    }
}

impl From<transfer::Len> for WireBytes {
    fn from(bytes: transfer::Len) -> Self {
        Self(bytes.into_raw())
    }
}

impl From<Resident> for ResidentSize {
    fn from(bytes: Resident) -> Self {
        Self(bytes.into_raw())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Body(bounded::Overflow),
    Total(bounded::Overflow),
}

/// Non-empty wire and bounded resident geometry proved for queue mutation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Geometry {
    wire: transfer::NonZero,
    resident: Resident,
}

impl Geometry {
    pub(in crate::link::egress) const fn new(wire: transfer::NonZero, resident: Resident) -> Self {
        Self { wire, resident }
    }

    pub const fn wire(self) -> transfer::NonZero {
        self.wire
    }

    pub const fn resident(self) -> Resident {
        self.resident
    }
}

/// Final ownership state of one enqueue attempt.
#[must_use = "an enqueue transition must be handled"]
pub enum Enqueue<B> {
    Committed,
    Rejected(Rejected<load::Value<B>>),
}

/// Exact drain state of one connection's admitted egress.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Pending {
    Absent,
    Present,
}

const _: () = {
    assert!(std::mem::size_of::<Pending>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<Pending>() == std::mem::align_of::<bool>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalid {
    Excess {
        actual: transfer::Len,
        maximum: transfer::Len,
    },
}

const _: () = assert!(std::mem::size_of::<Invalid>() == 2 * std::mem::size_of::<u32>());

/// Exact metadata-entry credit independent of resident-byte credit.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Entries(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deficit<T> {
    available: T,
    requested: T,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Overflow<T> {
    left: T,
    right: T,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResidentOut {
    actual: ResidentSize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shortage {
    Entries(Deficit<Entries>),
    Resident(Deficit<Resident>),
}

impl<T> Deficit<T> {
    const fn new(held: T, need: T) -> Self {
        Self {
            available: held,
            requested: need,
        }
    }

    pub const fn available(&self) -> &T {
        &self.available
    }

    pub const fn requested(&self) -> &T {
        &self.requested
    }
}

impl<T> Overflow<T> {
    const fn new(left: T, right: T) -> Self {
        Self { left, right }
    }

    pub const fn left(&self) -> &T {
        &self.left
    }

    pub const fn right(&self) -> &T {
        &self.right
    }
}

impl ResidentOut {
    const fn new(actual: usize) -> Self {
        Self {
            actual: ResidentSize::new(actual),
        }
    }

    pub const fn actual(self) -> ResidentSize {
        self.actual
    }
}

impl Entries {
    pub const fn new(entries: u32) -> Self {
        Self(entries)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn raw(self) -> u32 {
        self.0
    }

    const fn floor(self, lanes: metadata::LaneCount) -> Self {
        Self(self.0 / lanes.position().get())
    }

    const fn capped_add(self, added: Self) -> Self {
        Self(self.0.saturating_add(added.0))
    }

    pub(in crate::link::egress) const fn checked_sub(
        self,
        removed: Self,
    ) -> Result<Self, Deficit<Self>> {
        match self.0.checked_sub(removed.0) {
            Some(rest) => Ok(Self(rest)),
            None => Err(Deficit::new(self, removed)),
        }
    }
}

/// The live wait queue exhausted its serial domain. Existing waiters retain
/// their order; new serials become available when the waiting tree empties.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tickets;

/// Exact reason an egress credit request was not admitted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[must_use]
pub enum Admission {
    Tickets {
        lane: metadata::Lane,
        cause: Tickets,
    },
    Busy {
        lane: metadata::Lane,
        requested: Credit,
    },
    Queued {
        lane: metadata::Lane,
        requested: Credit,
        demand: Credit,
    },
    Credit {
        lane: metadata::Lane,
        requested: Credit,
        cause: fair::Cause<u32, 2>,
    },
    Storage {
        lane: metadata::Lane,
        requested: Credit,
        available: Entries,
    },
}

/// Exact reason an immutable payload load was rejected.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[must_use]
pub enum Rejection {
    Empty(Zero),
    Transfer(bounded::Overflow),
    Resident(ResidentOut),
    LaneCapacity {
        charge: Charge,
    },
    BatchCapacity {
        requested: Charge,
        remaining: Credit,
        cause: Shortage,
    },
    Capacity {
        charge: Charge,
        cause: Admission,
    },
    Changed {
        expected: Charge,
        actual: Charge,
    },
    LaneUnavailable,
}

/// An unchanged payload whose resident backing exceeds the requested bound.
#[must_use]
pub struct Excess<B> {
    pub load: Load<B>,
    pub maximum: usize,
}

/// Progress made while retiring one egress lane.
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use]
#[repr(u8)]
pub enum Cleared {
    /// The lane is empty and may be reused.
    Done,
    /// Retained entries remain, but this turn admitted no more cleanup work.
    Retry,
    /// A kernel flight still retains the lane.
    Waiting,
}

/// Result of retiring at most one owned entry from an egress lane.
#[must_use]
pub enum Clear<B> {
    /// The lane is empty and may be reused.
    Empty,
    /// One owned entry was released with the exact remaining lane state.
    Released { payload: B, remaining: Remaining },
    /// Retained entries remain, but this turn admitted no cleanup work.
    Retry,
    /// A kernel flight still retains the lane.
    Waiting,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Remaining {
    Empty,
    Pending,
}

pub(in crate::link::egress) enum Fit {
    Admitted,
    Entries,
    Resident,
}

const _: () = {
    assert!(usize::BITS >= u32::BITS);
    assert!(std::mem::size_of::<Entries>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<ResidentOut>() == std::mem::size_of::<ResidentSize>());
};

const _: () = {
    assert!(size_of::<ResidentSize>() == size_of::<usize>());
    assert!(size_of::<Geometry>() == 2 * size_of::<u32>());
    assert!(size_of::<Option<Geometry>>() == size_of::<Geometry>());
};

/// One exact two-axis egress credit quantity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Credit {
    entries: Entries,
    resident: Resident,
}

impl Credit {
    pub const fn new(entries: Entries, resident: Resident) -> Self {
        Self { entries, resident }
    }

    pub const fn fixed<const ENTRIES: u32, const RESIDENT: u32>() -> Self {
        Self::new(Entries::new(ENTRIES), Resident::new(RESIDENT))
    }

    pub const fn entries(self) -> Entries {
        self.entries
    }

    pub const fn resident(self) -> Resident {
        self.resident
    }

    pub(in crate::link::egress) const fn floor(self, lanes: metadata::LaneCount) -> Self {
        Self::new(self.entries.floor(lanes), self.resident.floor(lanes))
    }

    pub(in crate::link::egress) const fn capped_add(self, added: Self) -> Self {
        Self::new(
            self.entries.capped_add(added.entries),
            self.resident.capped_add(added.resident),
        )
    }
}

const _: () = {
    assert!(std::mem::size_of::<Admission>() == 5 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Option<Admission>>() == std::mem::size_of::<Admission>());
};
