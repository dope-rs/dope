use std::{marker, mem};

use o3::base::num;

use crate::{driver::route::indices, io::fd::handles};

/// Generation-checked descriptor identity, never descriptor authority.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct Identity<'d, S = handles::PhysicalSlot> {
    slot: S,
    generation: num::Generation,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Identity<'d> {
    pub(crate) fn new(slot: handles::PhysicalSlot, version: num::Generation) -> Self {
        Self {
            slot,
            generation: version,
            driver: marker::PhantomData,
        }
    }

    pub(crate) fn index(self) -> indices::SlotIndex {
        self.slot().into()
    }

    pub(crate) fn slot(self) -> handles::PhysicalSlot {
        self.slot
    }

    pub(crate) const fn slot_ref(&self) -> &handles::PhysicalSlot {
        &self.slot
    }
}

impl<'d> Identity<'d, handles::OutboundSlot> {
    pub(crate) fn outbound(slot: handles::OutboundSlot, version: num::Generation) -> Self {
        Self {
            slot,
            generation: version,
            driver: marker::PhantomData,
        }
    }

    pub(crate) fn physical(self) -> Identity<'d> {
        Identity::new(self.slot.physical(), self.generation)
    }

    pub(crate) fn slot(self) -> handles::OutboundSlot {
        self.slot
    }
}

impl<S> Identity<'_, S> {
    pub(crate) fn generation(self) -> num::Generation {
        self.generation
    }
}

const _: () = {
    assert!(mem::size_of::<Identity<'static>>() == mem::size_of::<u64>());
};
