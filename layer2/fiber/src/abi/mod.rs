pub mod future;
mod join;
pub mod machine;
mod pending;
mod pollfn;
pub mod race;
mod ready;
mod task;

use core::{pin, task::Poll};
use o3::{
    base::cell::region,
    mem::{self, slot},
};

pub use join::Join;
pub use pending::Pending;
pub use pollfn::{PollFn, poll_fn};
pub use ready::Ready;
pub use task::Task;

use crate::context;

#[derive(Clone, Copy, Debug)]
enum Side {
    Left,
    Right,
}

impl Side {
    const fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[must_use = "a fiber does nothing unless it is driven"]
/// A pinned unit of cooperative work admitted by its live owner and driver.
/// Ready ends that owner's polling authority; implementations do not duplicate it.
/// [`Task`] retains standalone completion; nested polls share admission and budget.
pub trait Fiber<'d>: Sized {
    type Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output>;
}

/// A pinned sequence whose owner stops polling after `Ready(None)`.
/// Each admitted call may yield one item while retaining the same stream owner.
/// `PollCall` admits work; it does not track stream completion.
#[must_use = "a stream does nothing unless it is driven"]
pub trait Stream<'d>: Sized {
    type Item;

    fn poll_next(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>>;
}

/// One admitted transition of a movable state in its exclusively owned slot.
pub trait Resume<'d>: Sized + Unpin {
    type Output;
    type Ready;

    fn poll<'slot, 'id>(
        live: &mut Live<'slot, 'id, Self>,
        admit: &mut context::Admission<'_, '_, 'd>,
    ) -> Poll<Self::Ready>;

    fn finish<'slot, 'id>(
        live: Live<'slot, 'id, Self>,
        ready: Self::Ready,
        admit: context::Admission<'_, '_, 'd>,
    ) -> Transition<'id, Self::Output>;
}

/// Owns a movable state until its consuming transition completes.
#[repr(transparent)]
#[must_use = "a task does nothing unless it is driven"]
pub struct Owned<F> {
    state: slot::Storage<F>,
}

impl<F> Owned<F> {
    pub const fn new(value: F) -> Self {
        Self {
            state: slot::Storage::filled(value, ()),
        }
    }
}

/// Admitted exclusive access to the current state in one owned slot.
pub struct Live<'slot, 'id, F> {
    slot: mem::raw::Occupied<'slot, F>,
    proof: region::Proof<region::Identity<'id>>,
}

impl<'slot, 'id, F> Live<'slot, 'id, F> {
    pub fn get_mut(&mut self) -> &mut F {
        self.slot.get_mut()
    }

    pub fn pending<O>(self) -> Transition<'id, O> {
        Transition {
            poll: Poll::Pending,
            _proof: self.proof,
        }
    }

    pub fn take(self) -> (Vacant<'slot, 'id, F>, F) {
        let (value, slot) = self.slot.take();
        (
            Vacant {
                slot,
                proof: self.proof,
            },
            value,
        )
    }
}

/// The original slot after its state has been consumed.
pub struct Vacant<'slot, 'id, F> {
    slot: mem::raw::Vacant<'slot, F>,
    proof: region::Proof<region::Identity<'id>>,
}

impl<'slot, 'id, F> Vacant<'slot, 'id, F> {
    pub fn insert(self, value: F) -> Live<'slot, 'id, F> {
        Live {
            slot: self.slot.insert(value),
            proof: self.proof,
        }
    }

    pub fn complete<O>(self, output: O) -> Transition<'id, O> {
        Transition {
            poll: Poll::Ready(output),
            _proof: self.proof,
        }
    }
}

/// A result issued only by the original live or emptied slot.
pub struct Transition<'id, O> {
    poll: Poll<O>,
    _proof: region::Proof<region::Identity<'id>>,
}

impl<'d, F: Resume<'d>> Fiber<'d> for Owned<F> {
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut admit) = call.into_admit();
        let Some(slot) = this.get_mut().state.occupied() else {
            return Poll::Pending;
        };
        region::Token::scope(|token| {
            let mut live = Live {
                slot,
                proof: token.proof(),
            };
            match F::poll(&mut live, &mut admit) {
                Poll::Pending => live.pending(),
                Poll::Ready(ready) => F::finish(live, ready, admit),
            }
            .poll
        })
    }
}

impl<'d, F: Fiber<'d>> Fiber<'d> for pin::Pin<&mut Task<F>> {
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        Task::<F>::poll(call.map(|fiber| fiber.get_mut().as_mut()))
    }
}
