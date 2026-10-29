use std::{iter::FusedIterator, ops::Range};

use crate::driver::route::indices;
pub mod borrows;
pub mod bounds;
mod capacity;
pub mod conn;
mod current;
pub mod entries;
mod key;
mod slab;

use std::{hash, marker};

use o3::{
    alloc,
    depot::arena::slab::{external, external::shared},
};

use crate::driver::route;

const PARTITIONS: usize = 2;

type Storage<T, Tag> = external::Exclusive<T, route::Epoch, Tag, { u32::MAX }, PARTITIONS>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Invalid {
    pub(in crate::driver) actual: usize,
    pub(in crate::driver) minimum: usize,
    pub(in crate::driver) maximum: usize,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Parts<Tag: route::Tag> {
    epoch: route::Epoch,
    slot: indices::SlotIndex,
    tag: marker::PhantomData<*mut Tag>,
}

impl Invalid {
    pub(crate) const fn from_bounds(actual: usize, minimum: usize, maximum: usize) -> Self {
        Self {
            actual,
            minimum,
            maximum,
        }
    }
}

impl<Tag: route::Tag> Parts<Tag> {
    pub const fn new(slot: indices::SlotIndex, epoch: route::Epoch) -> Self {
        Self {
            epoch,
            slot,
            tag: marker::PhantomData,
        }
    }

    pub const fn slot(self) -> indices::SlotIndex {
        self.slot
    }

    pub const fn epoch(self) -> route::Epoch {
        self.epoch
    }

    pub(in crate::driver::route::table) fn external(self) -> external::Key<route::Epoch, Tag> {
        external::Key::new(self.slot().raw(), self.epoch())
    }
}

impl<Tag: route::Tag> PartialEq for Parts<Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.slot == other.slot
    }
}

impl<Tag: route::Tag> Eq for Parts<Tag> {}

impl<Tag: route::Tag> hash::Hash for Parts<Tag> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.epoch.hash(state);
        self.slot.hash(state);
    }
}

pub use capacity::Capacity;
pub use current::Current;
pub use key::Key;
pub use slab::Slab;

const _: () = {
    assert!(std::mem::size_of::<Parts<route::KeyTag<1>>>() == 2 * std::mem::size_of::<u64>());
};

pub struct CellSlab<T, Tag: route::Tag> {
    inner: shared::Storage<T, route::Epoch, Tag>,
}

impl<T, Tag: route::Tag> CellSlab<T, Tag> {
    /// Reserves the complete interior-mutable target table transactionally.
    pub fn try_new(cap: Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: shared::Storage::try_new(cap.0)?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn raw(&self) -> route::raw::Cell<'_, T, Tag> {
        route::raw::Cell::new(&self.inner)
    }
}

#[derive(Clone, Debug)]
pub struct Iter(Range<u32>);

impl Iterator for Iter {
    type Item = indices::SlotIndex;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Self::slot)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth(n).map(Self::slot)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Self::slot)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth_back(n).map(Self::slot)
    }
}

impl Iter {
    fn slot(raw: u32) -> indices::SlotIndex {
        indices::SlotIndex::from_field(u64::from(raw))
    }
}

impl ExactSizeIterator for Iter {}
impl FusedIterator for Iter {}
