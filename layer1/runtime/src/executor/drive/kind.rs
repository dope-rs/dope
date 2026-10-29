use std::mem;

use dope_core::{
    Errno,
    driver::{life, result::poll, schedule::ready},
};
use poll::fault;

use crate::executor::drive::{clock, regress::Regression};

/// Borrowed cause of a drive failure.
#[derive(Debug, Eq, PartialEq)]
pub enum Kind<'a> {
    Boot(&'a life::Busy),
    Capacity(&'a ready::Capacity),
    Poll(&'a poll::Error),
    Completion(&'a fault::Dispatch),
    Deadline(&'a clock::Range),
    ClockMovedBackwards(&'a Regression),
    TimerDisarm(&'a Errno),
    Shutdown,
    QuiescenceTimeout,
    Timeout,
}

const _: () = {
    assert!(!mem::needs_drop::<Kind<'static>>());
    assert!(mem::size_of::<Kind<'static>>() <= mem::size_of::<usize>() * 2);
    assert!(mem::align_of::<Kind<'static>>() <= mem::align_of::<usize>());
};
