use std::{marker, mem, os::fd};

use dope_core::platform::{affinity, thread::Slot};

use crate::process::runtime::{
    bus,
    control::set,
    worker::task::{Output, Parent, Task},
};

type Slots<'id, I, E, X> = &'id [Slot<Task<I, E, X>, Output<X>, Parent>];

#[repr(transparent)]
pub(in crate::process::runtime) struct Bound<'id, I, E, X> {
    slots: Slots<'id, I, E, X>,
    owner: marker::PhantomData<fn(&'id ()) -> &'id ()>,
}

impl<'id, I, E, X> Bound<'id, I, E, X> {
    /// # Safety
    /// Every retained parent still owns its live Main endpoint.
    pub(in crate::process::runtime::worker) const unsafe fn new(
        slots: Slots<'id, I, E, X>,
    ) -> Self {
        Self {
            slots,
            owner: marker::PhantomData,
        }
    }

    fn slot(&self, index: set::Index<'id>) -> &'id Slot<Task<I, E, X>, Output<X>, Parent> {
        // SAFETY: Poll derives Index from this exact generative Bound's descriptors.
        unsafe { self.slots.get_unchecked(index.raw()) }
    }

    pub(in crate::process::runtime) fn main(&self, index: set::Index<'id>) -> &'id bus::Main {
        self.parent_main(self.slot(index).parent())
    }

    fn parent_main(&self, parent: &'id Parent) -> &'id bus::Main {
        // SAFETY: both callers select this parent from the admitted slot slice.
        // Main stays Some throughout this loan; stop and handoff require ownership.
        unsafe { parent.main.as_slice().get_unchecked(0) }
    }

    pub(in crate::process::runtime) fn cpu(&self, index: set::Index<'id>) -> affinity::Cpu {
        self.slot(index).parent().cpu
    }

    pub(in crate::process::runtime) fn fds<'fd>(
        &self,
    ) -> impl ExactSizeIterator<Item = fd::BorrowedFd<'fd>>
    where
        'id: 'fd,
    {
        self.slots
            .iter()
            .map(|slot| self.parent_main(slot.parent()).as_fd())
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<'static, (), (), ()>>() == mem::size_of::<&[()]>());
    assert!(mem::align_of::<Bound<'static, (), (), ()>>() == mem::align_of::<&[()]>());
};
