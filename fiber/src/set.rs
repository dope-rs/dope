use core::array::from_fn;
use core::marker::PhantomPinned;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::Poll;

use crate::{Context, Fiber};

/// A pinned, allocation-free homogeneous fiber set.
///
/// Unsafe pin projection and initialized-slot tracking stay inside
/// `dope-fiber`; applications only move fibers in before their first poll.
pub struct FiberSet<F, const N: usize> {
    slots: [MaybeUninit<F>; N],
    live: [bool; N],
    len: usize,
    _pin: PhantomPinned,
}

impl<F, const N: usize> FiberSet<F, N> {
    pub fn new() -> Self {
        assert!(N > 0, "fiber set capacity must be positive");
        Self {
            slots: from_fn(|_| MaybeUninit::uninit()),
            live: [false; N],
            len: 0,
            _pin: PhantomPinned,
        }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn try_push(self: Pin<&mut Self>, fiber: F) -> Result<(), F> {
        // SAFETY: inserting does not move any initialized slot, and the set is
        // pinned for the rest of the operation.
        let this = unsafe { self.get_unchecked_mut() };
        let Some(index) = this.live.iter().position(|live| !*live) else {
            return Err(fiber);
        };
        this.slots[index].write(fiber);
        this.live[index] = true;
        this.len += 1;
        Ok(())
    }

    pub fn poll_ready<'d>(
        self: Pin<&mut Self>,
        mut cx: Pin<&mut Context<'_, 'd>>,
        mut ready: impl FnMut(F::Output),
    ) -> usize
    where
        F: Fiber<'d>,
    {
        // SAFETY: the set is pinned and initialized entries never move.
        let this = unsafe { self.get_unchecked_mut() };
        let mut completed = 0;
        for index in 0..N {
            if !this.live[index] {
                continue;
            }
            // SAFETY: `live[index]` proves initialization; the set is pinned.
            let fiber = unsafe { this.slots[index].assume_init_mut() };
            // SAFETY: the initialized fiber remains at this address until its
            // terminal poll returns.
            if let Poll::Ready(output) =
                Fiber::poll(unsafe { Pin::new_unchecked(fiber) }, cx.as_mut())
            {
                // Clear liveness before running user Drop. If Drop unwinds,
                // the parent destructor cannot drop the same entry again.
                this.live[index] = false;
                this.len -= 1;
                completed += 1;
                // SAFETY: the slot was initialized and has just been marked dead.
                unsafe { this.slots[index].assume_init_drop() };
                ready(output);
            }
        }
        completed
    }
}

impl<F, const N: usize> Default for FiberSet<F, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F, const N: usize> Drop for FiberSet<F, N> {
    fn drop(&mut self) {
        for index in 0..N {
            if self.live[index] {
                // Make the destructor panic-safe for the same reason as the
                // terminal-poll path above.
                self.live[index] = false;
                self.len -= 1;
                // SAFETY: this entry was initialized and is dropped once.
                unsafe { self.slots[index].assume_init_drop() };
            }
        }
    }
}
