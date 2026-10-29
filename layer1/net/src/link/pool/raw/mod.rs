use std::{marker, mem};

use dope_core::{
    driver::route::{self, Identity, KeyTag, indices, table},
    io::socket,
};
use o3::{
    depot,
    depot::{capacity, index},
};

use crate::link::pool::{self, outbound::AddressTable};

mod prepared;

pub use prepared::Prepared;

// SAFETY: the retained source owns the exact allocated address cell and its exclusive loan.
unsafe impl<'a, 'd, ID: Identity, A: socket::Prepared> depot::raw::Project<usize>
    for &'a mut AddressTable<'d, ID, A>
{
    type Output = &'a mut mem::MaybeUninit<A>;

    unsafe fn project(self, index: usize) -> Self::Output {
        // SAFETY: the connection vacancy admits this paired immutable physical extent.
        unsafe { self.slots.get_unchecked_mut(index) }
    }
}

// SAFETY: Stored retains this exact address's initialization before connection publication.
unsafe impl<'a, 'd, ID: Identity, A: socket::Prepared>
    depot::raw::Project<usize, pool::Stored<'d, ID>> for &'a AddressTable<'d, ID, A>
{
    type Output = &'a A;

    unsafe fn project(self, index: usize) -> Self::Output {
        // SAFETY: the published connection retains this initialized address, and
        // this original table loan excludes recycling the paired physical lane.
        unsafe { self.slots.get_unchecked(index).assume_init_ref() }
    }
}

/// Sentinel-capable keyed-storage identity retaining complete connection authority.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Key<'d, ID: Identity> {
    key: pool::Key<'d, ID>,
}

/// Intrusive scheduler identity retaining its driver and route brands.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Physical<'d, ID: Identity> {
    lane: indices::SlotIndex,
    brand: marker::PhantomData<route::Target<'d, ID>>,
}

impl<ID: Identity> PartialEq for Key<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<ID: Identity> Eq for Key<'_, ID> {}

impl<ID: Identity> PartialEq for Physical<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.lane == other.lane
    }
}

impl<ID: Identity> Eq for Physical<'_, ID> {}

impl<'d, ID: Identity> Key<'d, ID> {
    pub const fn new(key: pool::Key<'d, ID>) -> Self {
        Self { key }
    }

    pub const fn into_key(self) -> pool::Key<'d, ID> {
        self.key
    }
}

impl<'d, ID: Identity> From<pool::Key<'d, ID>> for Key<'d, ID> {
    fn from(key: pool::Key<'d, ID>) -> Self {
        Self::new(key)
    }
}

// SAFETY: the niche raw form preserves the complete route target identity.
unsafe impl<'d, ID: Identity> depot::raw::Key for Key<'d, ID> {
    type Raw = route::raw::Identity<'d, ID>;
    type Live = Self;

    const VACANT: Self::Raw = Self::Raw::VACANT;

    fn index(self) -> usize {
        self.key.lane().index().get() as usize
    }

    fn into_raw(self) -> Self::Raw {
        Self::Raw::from_target(self.key.target())
    }

    fn into_live(self) -> Self::Live {
        self
    }

    fn from_raw(raw: index::Stored<Self>) -> Self {
        let raw = raw.into_raw();
        // SAFETY: Stored proves this target was emitted by a live Key.
        let target = unsafe { raw.target() };
        Self::new(pool::Key::from_target(target))
    }

    fn from_live(live: Self::Live) -> Self {
        live
    }

    fn decode(raw: Self::Raw) -> Option<Self> {
        raw.into_target()
            .map(|target| Self::new(pool::Key::from_target(target)))
    }
}

impl<'d, ID: Identity> Physical<'d, ID> {
    /// Projects a live connection key into its branded physical storage lane.
    pub const fn bind(key: pool::Key<'d, ID>) -> Self {
        Self {
            lane: key.lane(),
            brand: marker::PhantomData,
        }
    }

    pub(in crate::link) const fn lane(self) -> indices::SlotIndex {
        self.lane
    }
}

// SAFETY: every compact route slot round-trips with both owner brands intact;
// scalar conversions are deterministic and cannot panic or reenter storage.
unsafe impl<'d, ID: Identity> depot::raw::Compact for Physical<'d, ID> {
    const MAX: u32 = route::SLOT_MASK as u32;

    fn into_raw(self) -> u32 {
        self.lane.index().get()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        Self {
            // SAFETY: depot::raw::Compact bounds `raw` by Self::MAX.
            lane: unsafe { <indices::SlotIndex as depot::raw::Compact>::from_raw(raw) },
            brand: marker::PhantomData,
        }
    }
}

// SAFETY: route table construction bounds every position by Physical::MAX.
unsafe impl<'d, ID: Identity> depot::raw::Count<Physical<'d, ID>> for table::Capacity {
    fn count(self) -> capacity::Count {
        self.count()
    }
}

const _: () = {
    assert!(
        mem::size_of::<Key<'static, KeyTag<0>>>()
            == mem::size_of::<pool::Key<'static, KeyTag<0>>>()
    );
    assert!(
        mem::align_of::<Key<'static, KeyTag<0>>>()
            == mem::align_of::<pool::Key<'static, KeyTag<0>>>()
    );
    assert!(mem::size_of::<Physical<'static, KeyTag<0>>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Physical<'static, KeyTag<0>>>() == mem::align_of::<u32>());
};
