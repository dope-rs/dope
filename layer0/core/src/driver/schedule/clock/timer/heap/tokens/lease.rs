use std::{marker, num};

use o3::base::marker::ThreadBound;

use crate::driver::schedule::clock::timer::heap::tokens::Live;

/// Linear ownership of one resident slot within its owning store brand.
#[repr(transparent)]
pub struct Lease<'id, Tag = ()> {
    slot: num::NonZero<u32>,
    thread: ThreadBound,
    brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
}

impl<'id, Tag> Lease<'id, Tag> {
    pub(in crate::driver::schedule::clock::timer::heap) fn from_live(live: Live) -> Self {
        let slot = live.slot().raw() + 1;
        Self {
            // SAFETY: the live slot stays below u32::MAX, so the increment cannot wrap.
            slot: unsafe { num::NonZero::<u32>::new_unchecked(slot) },
            thread: ThreadBound::NEW,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::driver::schedule::clock::timer::heap) const fn live(&self) -> Live {
        Live((self.slot.get() - 1) as usize)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Lease<'static>>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Lease<'static>>() == std::mem::align_of::<u32>());
    assert!(std::mem::size_of::<Option<Lease<'static>>>() == std::mem::size_of::<u32>());
};
