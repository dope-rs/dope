use std::{marker, num, ptr, sync::atomic};

use dope_sys::io::reactor::complete::{self, Entry as _};

use crate::reactor::{cq, ring};

#[derive(Debug)]
pub(crate) struct State {
    head: ptr::NonNull<atomic::AtomicU32>,
    tail: ptr::NonNull<atomic::AtomicU32>,
    entries: ptr::NonNull<cq::Entry<()>>,
    mask: u32,
    local_head: u32,
    pub(super) available: u32,
}

pub struct Head<'a, T> {
    state: &'a mut State,
    ready: num::NonZero<u32>,
    token: marker::PhantomData<fn(T) -> T>,
}

impl<T> complete::Head for Head<'_, T> {
    type Entry = cq::Entry<T>;

    fn parts(&self) -> complete::Parts<T, cq::Outcome, cq::Flags>
    where
        T: complete::raw::Token,
    {
        // SAFETY: the acquired nonzero count proves this CQE is initialized;
        // the exclusive queue borrow prevents its head from being consumed.
        unsafe { self.state.read::<T>() }.into_parts()
    }

    fn commit(self) {
        self.state.local_head = self.state.local_head.wrapping_add(1);
        self.state.available = self.ready.get() - 1;
        self.state.publish();
    }
}

impl State {
    fn offset(&self) -> usize {
        (self.local_head & self.mask) as usize
    }

    /// # Safety
    /// The acquired tail proves the current head is initialized and unconsumed.
    unsafe fn read<T>(&self) -> cq::Entry<T> {
        unsafe {
            self.entries
                .as_ptr()
                .add(self.offset())
                .cast::<cq::Entry<T>>()
                .read()
        }
    }

    /// # Safety
    /// The mapping and parameters must describe one live kernel-created ring
    /// and outlive this state.
    pub(crate) unsafe fn new(ring: &ring::ffi::Map, layout: ring::Complete) -> Self {
        let head = ring.at(layout.head());
        let tail = ring.at(layout.tail());
        let head_at = unsafe { head.as_ref().load(atomic::Ordering::Relaxed) };
        let tail_at = unsafe { tail.as_ref().load(atomic::Ordering::Acquire) };
        let axis: ring::Axis = layout.axis();
        Self {
            head,
            tail,
            entries: ring.at(layout.entries()),
            mask: axis.mask(),
            local_head: head_at,
            available: tail_at.wrapping_sub(head_at),
        }
    }

    pub(crate) fn publish(&mut self) {
        // SAFETY: construction binds both atomics to this live ring mapping.
        unsafe { self.head.as_ref() }.store(self.local_head, atomic::Ordering::Release);
    }

    pub(crate) fn acquire(&mut self) {
        self.available = self.observed();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.available == 0
    }

    pub(crate) fn observed(&self) -> u32 {
        // SAFETY: the ring mapping owns this live kernel publication atomic.
        let tail = unsafe { self.tail.as_ref() }.load(atomic::Ordering::Acquire);
        tail.wrapping_sub(self.local_head)
    }

    pub(crate) fn front<T>(&mut self) -> Option<Head<'_, T>> {
        let ready = num::NonZero::new(self.observed())?;
        Some(Head {
            state: self,
            ready,
            token: marker::PhantomData,
        })
    }

    pub(crate) fn pop<T>(&mut self) -> Option<cq::Entry<T>> {
        self.available = self.available.checked_sub(1)?;
        // SAFETY: the acquired tail publishes this initialized CQE and this
        // consumer owns the selected head slot until its release sync.
        let entry = unsafe { self.read() };
        self.local_head = self.local_head.wrapping_add(1);
        Some(entry)
    }
}
