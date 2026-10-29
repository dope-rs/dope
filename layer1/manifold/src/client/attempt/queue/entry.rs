use std::mem::{self, ManuallyDrop};

use dope_core::driver::route::Identity;
use dope_net::link::pool;

use crate::client::attempt;

/// Each variant carries its own header, keeping a four-byte-aligned plan next
/// to the tag without adding the binding's eight-byte alignment before it.
#[repr(C)]
struct Queued<T: dope_net::Transport> {
    phase: u32,
    plan: attempt::Plan<T>,
}

#[repr(C)]
struct Bound<'d, ID: Identity> {
    phase: u32,
    key: pool::Key<'d, ID>,
}

#[repr(C)]
pub(in crate::client::attempt::queue) union Entry<'d, T: dope_net::Transport, ID: Identity> {
    phase: Phase,
    pending: Pending,
    queued: ManuallyDrop<Queued<T>>,
    bound: ManuallyDrop<Bound<'d, ID>>,
}

pub(super) enum Fail {
    Keep,
    Remove,
}

pub(super) enum Change {
    Keep,
    Remove,
    Queue,
}

#[derive(Clone, Copy)]
#[repr(u32)]
enum Phase {
    Queued,
    Opening,
    Submitted,
    Connected,
    Active,
    Cancelled,
    Done,
}

/// The phases retained by a live, affine attempt lease.
#[derive(Clone, Copy)]
#[repr(u32)]
pub(super) enum Pending {
    Queued = Phase::Queued as u32,
    Opening = Phase::Opening as u32,
    Submitted = Phase::Submitted as u32,
    Connected = Phase::Connected as u32,
    Done = Phase::Done as u32,
}

impl<'d, T: dope_net::Transport, ID: Identity> Entry<'d, T, ID> {
    pub(in crate::client::attempt::queue) fn queued(plan: attempt::Plan<T>) -> Self {
        const { assert!(!mem::needs_drop::<Queued<T>>()) };
        Self {
            queued: ManuallyDrop::new(Queued {
                phase: Pending::Queued as u32,
                plan,
            }),
        }
    }

    fn phase(&self) -> Phase {
        // SAFETY: every variant has the same initialized repr(C) header.
        unsafe { self.phase }
    }

    pub(in crate::client::attempt::queue) fn is_owned(&self) -> bool {
        matches!(self.phase(), Phase::Active | Phase::Cancelled)
    }

    pub(in crate::client::attempt::queue) fn done(&mut self) {
        self.pending = Pending::Done;
    }

    pub(in crate::client::attempt::queue) fn fail(&mut self) -> Fail {
        match self.phase() {
            Phase::Opening | Phase::Submitted => {
                self.done();
                Fail::Keep
            }
            Phase::Cancelled => Fail::Remove,
            Phase::Queued | Phase::Connected | Phase::Active | Phase::Done => Fail::Keep,
        }
    }

    /// # Safety
    /// The caller consumes this exact resident's sole Pending lease proof.
    pub(in crate::client::attempt::queue) unsafe fn finish(&mut self, commit: bool) -> Change {
        // SAFETY: the consumed lease excludes Active and Cancelled. Read the
        // narrow tag by value; no Pending reference survives the transition.
        match unsafe { self.pending } {
            Pending::Connected if commit => {
                self.phase = Phase::Active;
                Change::Keep
            }
            Pending::Submitted | Pending::Connected => {
                self.phase = Phase::Cancelled;
                Change::Queue
            }
            Pending::Queued | Pending::Opening | Pending::Done => Change::Remove,
        }
    }

    /// # Safety
    /// The caller consumes lane 0 membership of this exact queued resident.
    pub(in crate::client::attempt::queue) unsafe fn open(&mut self) -> attempt::Plan<T> {
        // SAFETY: lane 0's publication and defer producers store Queued first.
        let queued = unsafe { ManuallyDrop::take(&mut self.queued) };
        self.pending = Pending::Opening;
        queued.plan
    }

    pub(in crate::client::attempt::queue) fn connected(&mut self) -> attempt::Transition {
        if matches!(self.phase(), Phase::Submitted) {
            self.pending = Pending::Connected;
            attempt::Transition::Applied
        } else {
            attempt::Transition::Stale
        }
    }

    pub(in crate::client::attempt::queue) fn defer(
        &mut self,
        plan: attempt::Plan<T>,
    ) -> attempt::Transition {
        if matches!(self.phase(), Phase::Opening) {
            *self = Self::queued(plan);
            attempt::Transition::Applied
        } else {
            attempt::Transition::Stale
        }
    }

    pub(in crate::client::attempt::queue) fn reject(&mut self) {
        if matches!(self.phase(), Phase::Opening) {
            self.done();
        }
    }

    pub(in crate::client::attempt::queue) fn bind(&mut self, key: pool::Key<'d, ID>) {
        const { assert!(!mem::needs_drop::<Bound<'d, ID>>()) };
        if matches!(self.phase(), Phase::Opening) {
            *self = Self {
                bound: ManuallyDrop::new(Bound {
                    phase: Pending::Submitted as u32,
                    key,
                }),
            };
        }
    }

    /// # Safety
    /// The caller consumes lane 1 membership of this exact cancelled resident.
    pub(in crate::client::attempt::queue) unsafe fn cancel(&self) -> pool::Key<'d, ID> {
        // SAFETY: finish is lane 1's sole producer and preserves the binding.
        unsafe { self.bound.key }
    }
}
