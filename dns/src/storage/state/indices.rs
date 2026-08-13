use std::{io, marker};

use o3::collections::{self, batch::set};

use crate::{storage::state, wire};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::storage) struct LaneIndex(pub(in crate::storage::state) u16);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::storage) struct FamilyKey(pub(in crate::storage::state) u16);

#[derive(Clone, Copy)]
pub(in crate::storage) struct IndexLayout {
    lanes: IndexBound<LaneIndex>,
    families: IndexBound<FamilyKey>,
}

#[derive(Clone, Copy)]
pub(in crate::storage) struct IndexBound<I> {
    max: u16,
    index: marker::PhantomData<fn(I) -> I>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::storage) struct ServerIndex(u8);

impl LaneIndex {
    pub(in crate::storage) const fn get(self) -> usize {
        self.0 as usize
    }
}

impl set::DenseIndex for LaneIndex {
    fn into_usize(self) -> usize {
        self.get()
    }

    fn from_usize(raw: usize) -> Self {
        Self(raw as u16)
    }
}

impl FamilyKey {
    pub(in crate::storage) const fn new(lane: LaneIndex, family: state::Family) -> Self {
        Self((lane.0 << 1) | family as u16)
    }

    pub(in crate::storage) const fn lane(self) -> LaneIndex {
        LaneIndex(self.0 >> 1)
    }

    pub(in crate::storage) const fn family(self) -> state::Family {
        state::Family::from_bit(self.0 & 1)
    }

    pub(in crate::storage) const fn dense(self) -> u16 {
        self.0
    }
}

impl set::DenseIndex for FamilyKey {
    fn into_usize(self) -> usize {
        self.0 as usize
    }

    fn from_usize(raw: usize) -> Self {
        Self(raw as u16)
    }
}

impl IndexLayout {
    pub(in crate::storage) fn new(lanes: usize) -> io::Result<Self> {
        if lanes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS lane bound must be nonzero",
            ));
        }
        let Some(families) = lanes.checked_mul(state::Family::COUNT) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS lane bound exceeds the addressable capacity",
            ));
        };
        if families >= usize::from(u16::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS lane bound exhausts the family key space",
            ));
        }
        Ok(Self {
            lanes: IndexBound::new((lanes - 1) as u16),
            families: IndexBound::new((families - 1) as u16),
        })
    }

    pub(in crate::storage) const fn lanes(self) -> IndexBound<LaneIndex> {
        self.lanes
    }

    pub(in crate::storage) const fn families(self) -> IndexBound<FamilyKey> {
        self.families
    }

    pub(in crate::storage) const fn transactions(self) -> IndexBound<wire::TransactionId> {
        IndexBound::new(u16::MAX)
    }
}

impl<I> IndexBound<I> {
    const fn new(max: u16) -> Self {
        Self {
            max,
            index: marker::PhantomData,
        }
    }

    pub(in crate::storage) const fn max(self) -> u16 {
        self.max
    }

    pub(in crate::storage) const fn capacity(self) -> usize {
        self.max as usize + 1
    }
}

impl<I: DenseIndex> IndexBound<I> {
    pub(in crate::storage) fn try_filled_set(
        self,
    ) -> Result<set::Set<I>, collections::AllocationError> {
        let set = set::Set::try_with_capacity(self.capacity())?;
        for raw in 0..=self.max() {
            assert!(set.insert(I::from_dense(raw)));
        }
        Ok(set)
    }
}

pub(in crate::storage) trait DenseIndex: set::DenseIndex {
    fn from_dense(raw: u16) -> Self;
}

impl DenseIndex for LaneIndex {
    fn from_dense(raw: u16) -> Self {
        Self(raw)
    }
}

impl DenseIndex for FamilyKey {
    fn from_dense(raw: u16) -> Self {
        Self(raw)
    }
}

impl DenseIndex for wire::TransactionId {
    fn from_dense(raw: u16) -> Self {
        Self::from_wire(raw)
    }
}

const _: () = assert!(std::mem::size_of::<FamilyKey>() == std::mem::size_of::<u16>());
const _: () = assert!(std::mem::size_of::<IndexBound<FamilyKey>>() == std::mem::size_of::<u16>());
const _: () = assert!(IndexBound::<wire::TransactionId>::new(u16::MAX).capacity() == 1 << 16);
const _: () = assert!(state::Family::V4.index() == 0 && state::Family::V6.index() == 1);
const _: () = {
    let lane = LaneIndex(32_766);
    assert!(FamilyKey::new(lane, state::Family::V4).0 == 65_532);
    assert!(FamilyKey::new(lane, state::Family::V6).0 == 65_533);
    assert!(FamilyKey::new(lane, state::Family::V6).0 != u16::MAX);
};

impl ServerIndex {
    pub(in crate::storage) const ZERO: Self = Self(0);

    pub(in crate::storage) fn new(index: usize) -> Option<Self> {
        u8::try_from(index).ok().map(Self)
    }

    pub(in crate::storage) const fn get(self) -> usize {
        self.0 as usize
    }

    pub(in crate::storage) fn next(self, servers: usize) -> Self {
        debug_assert!(servers != 0);
        Self(((self.get() + 1) % servers) as u8)
    }
}
