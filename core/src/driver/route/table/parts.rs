use core::{hash, marker};

use crate::driver::route;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Parts<Tag: route::Tag> {
    epoch: route::Epoch,
    slot: route::SlotIndex,
    tag: marker::PhantomData<*mut Tag>,
}

impl<Tag: route::Tag> Parts<Tag> {
    pub(in crate::driver::route) const fn new(index: u32, epoch: u64) -> Option<Self> {
        let Some(slot) = route::SlotIndex::try_new(index) else {
            return None;
        };
        let Some(epoch) = route::Epoch::new(epoch) else {
            return None;
        };
        Some(Self::from_components(slot, epoch))
    }

    pub const fn from_components(slot: route::SlotIndex, epoch: route::Epoch) -> Self {
        Self {
            epoch,
            slot,
            tag: marker::PhantomData,
        }
    }

    pub const fn index(self) -> u32 {
        self.slot.raw()
    }

    pub const fn slot(self) -> route::SlotIndex {
        self.slot
    }

    pub const fn epoch(self) -> route::Epoch {
        self.epoch
    }

    pub const fn generation(self) -> route::Epoch {
        self.epoch
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

const _: () = {
    assert!(core::mem::size_of::<Parts<route::KeyTag<1>>>() == 2 * core::mem::size_of::<u64>());
};
