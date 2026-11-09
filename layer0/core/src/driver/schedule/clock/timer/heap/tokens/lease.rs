use std::{marker, num};

use o3::base::marker::ThreadBound;

use crate::driver::schedule::clock::timer::heap::tokens::Slot;

/// Linear ownership of one resident slot within its owning store brand.
#[repr(transparent)]
pub struct Lease<'id, Tag = ()> {
    slot: num::NonZero<u32>,
    thread: ThreadBound,
    brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
}

impl<'id, Tag> Lease<'id, Tag> {
    pub(in crate::driver::schedule::clock::timer::heap) fn from_slot(slot: Slot) -> Self {
        let slot = slot.raw() + 1;
        Self {
            // SAFETY: the live slot stays below u32::MAX, so the increment cannot wrap.
            slot: unsafe { num::NonZero::<u32>::new_unchecked(slot) },
            thread: ThreadBound::NEW,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::driver::schedule::clock::timer::heap) const fn slot(&self) -> Slot {
        Slot::from_stored(self.slot.get() - 1)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Lease<'static>>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Lease<'static>>() == std::mem::align_of::<u32>());
    assert!(std::mem::size_of::<Option<Lease<'static>>>() == std::mem::size_of::<u32>());
};
