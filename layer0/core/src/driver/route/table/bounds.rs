use dope_sys::probe::accept;

use crate::driver::route::{indices, table};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Outside {
    actual: usize,
    limit: usize,
}

/// An ABI-valid slot excluded by one concrete table capacity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Excluded {
    actual: indices::SlotIndex,
    capacity: table::Capacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    Abi(Outside),
    Capacity(Outside),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SegmentError {
    base: indices::Edge,
    capacity: table::Capacity,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Offset {
    Before {
        actual: indices::SlotIndex,
        base: indices::Edge,
    },
    Capacity(Excluded),
}

/// A route segment whose complete physical range is representable.
#[derive(Clone, Copy)]
pub struct Segment {
    base: indices::Edge,
    capacity: table::Capacity,
}

/// One local position and its physical route slot, projected together by a
/// validated segment. Accessors preserve this exact projection without checks.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Index {
    local: indices::SlotIndex,
    physical: indices::SlotIndex,
}

impl Index {
    pub const fn local(self) -> indices::SlotIndex {
        self.local
    }

    pub const fn physical(self) -> indices::SlotIndex {
        self.physical
    }
}

impl Outside {
    pub(super) const fn new(actual: usize, limit: usize) -> Self {
        Self { actual, limit }
    }
}

impl From<accept::Outside> for Slot {
    fn from(error: accept::Outside) -> Self {
        match error {
            accept::Outside::Abi(actual) => Self::Abi(Outside::new(actual.get() as usize, 1 << 24)),
            accept::Outside::Capacity { actual, limit } => {
                Self::Capacity(Outside::new(actual.get() as usize, limit.get() as usize))
            }
        }
    }
}

impl Excluded {
    pub(super) const fn new(actual: indices::SlotIndex, capacity: table::Capacity) -> Self {
        Self { actual, capacity }
    }
}

impl Segment {
    pub fn try_new(base: indices::Edge, capacity: table::Capacity) -> Result<Self, SegmentError> {
        if capacity
            .into_iter()
            .next_back()
            .is_some_and(|last| base.checked_add(last).is_none())
        {
            return Err(SegmentError { base, capacity });
        }
        Ok(Self { base, capacity })
    }

    pub const fn capacity(self) -> table::Capacity {
        self.capacity
    }

    pub fn try_index(self, local: indices::SlotIndex) -> Result<Index, Excluded> {
        if let Some(outside) = self.capacity.excludes(local) {
            return Err(outside);
        }
        Ok(self.project(local))
    }

    pub fn indices(self) -> impl DoubleEndedIterator<Item = Index> + ExactSizeIterator {
        self.capacity
            .into_iter()
            .map(move |local| self.project(local))
    }

    fn project(self, local: indices::SlotIndex) -> Index {
        let raw = u64::from(self.base.raw()) + u64::from(local.raw());
        Index {
            local,
            physical: indices::SlotIndex::from_field(raw),
        }
    }

    pub fn offset(self, physical: indices::SlotIndex) -> Result<Index, Offset> {
        let Some(local) = self.base.offset(physical) else {
            return Err(Offset::Before {
                actual: physical,
                base: self.base,
            });
        };
        if !self.capacity.contains(local) {
            return Err(Offset::Capacity(Excluded::new(local, self.capacity)));
        }
        Ok(Index { local, physical })
    }
}
