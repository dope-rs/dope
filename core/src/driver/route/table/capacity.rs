use o3::collections::slab;

use crate::driver::route;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct Capacity(pub(in crate::driver::route::table) slab::Capacity);

impl Capacity {
    pub const EMPTY: Self = Self(slab::Capacity::EMPTY);

    pub const fn new(capacity: usize) -> Option<Self> {
        if capacity <= route::SLOT_MASK as usize + 1 {
            Some(Self(slab::Capacity::new(capacity as u32)))
        } else {
            None
        }
    }

    pub(in crate::driver) const fn fixed(capacity: u32) -> Self {
        assert!(capacity <= route::SLOT_MASK as u32 + 1);
        Self(slab::Capacity::new(capacity))
    }

    pub const fn get(self) -> usize {
        self.0.get()
    }

    pub const fn raw(self) -> u32 {
        self.0.raw()
    }

    pub const fn slot(self, index: usize) -> Option<route::SlotIndex> {
        if index < self.get() {
            Some(route::SlotIndex::from_bounded(index as u32))
        } else {
            None
        }
    }

    pub fn slots(self) -> impl ExactSizeIterator<Item = route::SlotIndex> + DoubleEndedIterator {
        (0..self.raw()).map(route::SlotIndex::from_bounded)
    }
}
