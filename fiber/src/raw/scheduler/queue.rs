use core::{marker, pin, ptr};

use o3::collections::{self, batch::set, slab};

pub(crate) struct ReadyQueue {
    ready: set::Set<u32>,
    _pin: marker::PhantomPinned,
}

impl ReadyQueue {
    pub(crate) fn try_with_capacity(
        capacity: slab::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            ready: set::Set::try_with_capacity(capacity.get())?,
            _pin: marker::PhantomPinned,
        })
    }

    pub(crate) fn is_empty(self: pin::Pin<&Self>) -> bool {
        self.ready.is_empty()
    }

    pub(crate) fn contains(self: pin::Pin<&Self>, index: u32) -> bool {
        self.ready.contains(index)
    }

    pub(crate) fn snapshot(self: pin::Pin<&Self>) -> Option<set::Drain<'_, u32>> {
        self.get_ref().ready.drain_batch()
    }

    pub(crate) fn return_ready(self: pin::Pin<&Self>, index: u32) -> bool {
        self.ready.insert(index)
    }
}

// SAFETY: Scheduler owns this pinned queue after its binding slots and validates
// every index against both collections before binding it.
unsafe impl crate::raw::BindingQueue for ReadyQueue {
    type Input = ();

    fn attach(self: pin::Pin<&Self>, index: usize, (): Self::Input) -> usize {
        index
    }

    fn ready(self: pin::Pin<&Self>) -> pin::Pin<&set::Set<usize>> {
        // SAFETY: `ready` is structurally pinned with its queue.
        let ready = unsafe { self.map_unchecked(|queue| &queue.ready) };
        // SAFETY: Set<I> is transparent over identical storage for every I.
        // Bindings use only indices admitted by this u32 set's capacity.
        unsafe { ready.map_unchecked(|set| &*ptr::from_ref(set).cast::<set::Set<usize>>()) }
    }
}

const _: () = {
    assert!(core::mem::size_of::<ReadyQueue>() == core::mem::size_of::<set::Set<u32>>());
    assert!(core::mem::size_of::<set::Set<u32>>() == core::mem::size_of::<set::Set<usize>>());
    assert!(
        core::mem::size_of::<set::Drain<'static, u32>>()
            == core::mem::size_of::<set::Drain<'static, usize>>()
    );
};
