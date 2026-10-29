use std::{marker::PhantomData, pin::Pin, ptr::NonNull};

use crate::task::storage::{fixed::schedule::ready::owned, set::ready::owners};

pub(super) struct Ready<'s, 'd> {
    queue: NonNull<owned::Queue<'d, owners::Source>>,
    scope: PhantomData<fn(&'s mut ()) -> &'s mut ()>,
}

impl<'s, 'd> Ready<'s, 'd> {
    /// # Safety
    /// Keep this exact queue pinned and live during every access through this
    /// owner. Restrict the owner to a fresh scope that cannot escape the queue.
    pub(super) unsafe fn new(queue: Pin<&owned::Queue<'d, owners::Source>>) -> Self {
        Self {
            queue: NonNull::from(queue.get_ref()),
            scope: PhantomData,
        }
    }

    pub(super) fn get(&self) -> Pin<&owned::Queue<'d, owners::Source>> {
        // SAFETY: construction retains this exact pinned queue for the scoped
        // owner; the returned reference cannot outlive this owner's borrow.
        unsafe { Pin::new_unchecked(self.queue.as_ref()) }
    }
}
