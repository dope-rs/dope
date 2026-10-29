use std::time;

use crate::{
    Errno,
    table::change::{Filter, Index},
};

mod backend;
pub mod delivery;
mod driver;
mod poll;
pub mod raw;
mod source;
pub mod store;

pub use backend::Backend;
pub use driver::Driver;
pub use poll::Poll;
pub use source::Source;

/// Readiness failures classified at the syscall or timeout validation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    Timeout(time::Duration),
    Wait(Errno),
    Changes(Errno),
}

/// One syscall's bounded receipt storage, shared by its native input and output.
pub const RECEIPTS: usize = 64;

/// Every failure issued by one retirement attempt, in receipt order.
#[derive(Debug)]
pub struct Failures<F> {
    pub first: F,
    pub rest: [Option<F>; RECEIPTS - 1],
}

impl<F> From<F> for Failures<F> {
    fn from(first: F) -> Self {
        Self {
            first,
            rest: [const { None }; RECEIPTS - 1],
        }
    }
}

/// Exact readiness failure constructors retaining the caller's typed domains.
pub trait Failure: Copy + From<Cause> {
    type Index: Copy + Eq;
    type Missing: Copy + Eq;
    type Operation: Copy;

    fn revoke(fd: usize, filter: Filter, errno: Errno) -> Self;
    fn backing(error: Index<Self::Index>) -> Self;
    fn missing(op: Self::Operation, cause: Self::Missing) -> Self;
    fn system(op: Self::Operation, errno: Errno) -> Self;
}
