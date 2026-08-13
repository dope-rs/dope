//! Scheduler wait evidence.

use std::{marker, time};

use o3::cell::region;

/// Driver-scoped evidence that a retained owner is waiting for an external
/// wake or a monotonic deadline.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Wait<'d> {
    deadline: Option<time::Instant>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Wait<'d> {
    pub fn event(_region: &region::Token<'d>) -> Self {
        Self {
            deadline: None,
            _driver: marker::PhantomData,
        }
    }

    pub fn until(_region: &region::Token<'d>, deadline: time::Instant) -> Self {
        Self {
            deadline: Some(deadline),
            _driver: marker::PhantomData,
        }
    }

    pub const fn deadline(self) -> Option<time::Instant> {
        self.deadline
    }

    pub(super) fn reduce(self, other: Self) -> Self {
        let deadline = match (self.deadline, other.deadline) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        Self {
            deadline,
            _driver: marker::PhantomData,
        }
    }
}
