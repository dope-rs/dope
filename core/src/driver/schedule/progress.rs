use std::{mem, time};

use o3::cell::region;

use crate::driver::schedule;

/// Aggregate liveness of retained work owned by one driver.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress<'d> {
    Runnable,
    Waiting(schedule::Wait<'d>),
    Quiescent,
}

impl<'d> Progress<'d> {
    pub fn waiting(region: &region::Token<'d>) -> Self {
        Self::Waiting(schedule::Wait::event(region))
    }

    pub fn until(region: &region::Token<'d>, deadline: time::Instant) -> Self {
        Self::Waiting(schedule::Wait::until(region, deadline))
    }

    pub fn reduce(self, other: Self) -> Self {
        match (self, other) {
            (Self::Runnable, _) | (_, Self::Runnable) => Self::Runnable,
            (Self::Waiting(left), Self::Waiting(right)) => Self::Waiting(left.reduce(right)),
            (waiting @ Self::Waiting(_), Self::Quiescent)
            | (Self::Quiescent, waiting @ Self::Waiting(_)) => waiting,
            (Self::Quiescent, Self::Quiescent) => Self::Quiescent,
        }
    }
}

const _: () = assert!(mem::size_of::<Progress<'static>>() <= 24);
