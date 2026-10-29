use std::{marker, ptr};

use dope_sys::{io::reactor::ready::RECEIPTS, table::change};

use crate::store::change::Changes;

/// Exclusively reserves the first staged_len active entries in their original order.
#[must_use = "a successful kqueue submission must commit its changelist"]
#[repr(transparent)]
pub struct Tail<'a, S: change::raw::Slots>(pub(super) &'a mut Changes<S>);

/// A subslice retaining its original bounded changelist borrow.
pub(crate) struct Batch<'a, S: change::raw::Slots> {
    events: &'a mut [libc::kevent],
    slots: marker::PhantomData<fn() -> S>,
}

impl<S: change::raw::Slots> Tail<'_, S> {
    pub(crate) fn changes(&mut self) -> Batch<'_, S> {
        // SAFETY: staging bounds this prefix by its fixed owner extent;
        // Tail retains that unchanged allocation through the exclusive borrow.
        Batch::new(unsafe { self.0.staged.get_unchecked_mut(..self.0.staged_len) })
    }

    /// There is at most one staged change per descriptor/filter. Partitioning
    /// those independent changes preserves the original membership commit proof.
    pub(crate) fn split(&mut self) -> (Batch<'_, S>, Batch<'_, S>) {
        let deleting = self.0.staged_delete;
        let staged = self.changes().events;
        let mut deletes = 0;
        if deleting {
            for index in 0..staged.len() {
                if staged[index].flags & libc::EV_DELETE != 0 {
                    if index != deletes {
                        let events = staged.as_mut_ptr();
                        // SAFETY: previous matches give deletes <= index < staged.len();
                        // this branch swaps distinct entries in the same exclusive slice.
                        unsafe { ptr::swap(events.add(index), events.add(deletes)) };
                    }
                    deletes += 1;
                }
            }
        }
        // SAFETY: at most one deletion was counted for each event in this
        // unchanged exclusive slice, so deletes <= staged.len().
        let (deletes, updates) = unsafe { staged.split_at_mut_unchecked(deletes) };
        (Batch::new(deletes), Batch::new(updates))
    }
}

impl<'a, S: change::raw::Slots> Batch<'a, S> {
    fn new(events: &'a mut [libc::kevent]) -> Self {
        Self {
            events,
            slots: marker::PhantomData,
        }
    }

    pub(crate) fn receipts(self) -> impl Iterator<Item = Self> {
        self.events.chunks_mut(RECEIPTS).map(|events| {
            for event in &mut *events {
                event.flags |= libc::EV_RECEIPT;
            }
            Self::new(events)
        })
    }
}

impl<S: change::raw::Slots> change::Tail for Tail<'_, S> {
    fn is_empty(&self) -> bool {
        self.0.staged_len == 0
    }

    fn len(&self) -> usize {
        self.0.staged_len
    }

    fn as_mut_slice(&mut self) -> &mut [libc::kevent] {
        self.0.staged_delete = true;
        self.changes().events
    }

    fn commit(self) {
        self.0.remove(self.0.staged_len);
        self.0.staged_len = 0;
    }

    fn retry(self) {
        self.0.staged_len = 0;
    }
}

// SAFETY: staging emits at most two changes per slot of the bounded table.
unsafe impl<S: change::raw::Slots> change::raw::Batch for Tail<'_, S> {
    type Event = libc::kevent;

    const MAX: usize = 2 * S::MAX;

    fn as_slice(&self) -> &[libc::kevent] {
        // SAFETY: Tail retains the original fixed staging owner; only bounded
        // staging advances staged_len, and this shared borrow cannot change it.
        unsafe { self.0.staged.get_unchecked(..self.0.staged_len) }
    }
}

// SAFETY: only subslices of the exclusively borrowed, bounded Tail are admitted.
unsafe impl<S: change::raw::Slots> change::raw::Batch for Batch<'_, S> {
    type Event = libc::kevent;

    const MAX: usize = 2 * S::MAX;

    fn as_slice(&self) -> &[libc::kevent] {
        self.events
    }
}
