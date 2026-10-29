mod context;
mod guard;
mod options;
pub mod raw;
mod reserve;

pub use context::Context;
pub use guard::Guard;
pub use options::Options;
pub use reserve::Reserve;

use crate::io::reactor::Full;

/// Reserves native capacity without consuming the host's operation ownership.
pub trait Admit<C: Context>: Sized {
    type Admission<'a>: Guard<C> + raw::Retain<C>
    where
        Self: 'a;

    fn try_admit(&mut self) -> Result<Self::Admission<'_>, Full>;
}

/// A key-bound cancellation whose native preparation has already succeeded.
pub trait Submission: Sized {
    fn submit(self) -> Cancelled;
}

/// Class-specific native admission chosen entirely through its GAT.
pub trait Plan {
    type Guard<'a, C: Context, B: Backend<C> + 'a>: Reserve<'a, C, B>;
}

/// Selects a pending work family without a runtime class token.
pub struct Pending<const FAMILY: u8>;
/// Synchronous work may already own a completion in the native queue.
pub struct Immediate;

pub trait Backend<C: Context>: Sized {
    type Pending<'a, const FAMILY: u8>: Reserve<'a, C, Self>
    where
        Self: 'a;
    type Immediate<'a>: Reserve<'a, C, Self>
    where
        Self: 'a;
}

impl<const FAMILY: u8> Plan for Pending<FAMILY> {
    type Guard<'a, C: Context, B: Backend<C> + 'a> = B::Pending<'a, FAMILY>;
}

impl Plan for Immediate {
    type Guard<'a, C: Context, B: Backend<C> + 'a> = B::Immediate<'a>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cancelled {
    Submitted,
    Queued,
    Completed,
}

/// The native queue does not own the requested completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Absent;

/// The reserved native cancellation command was submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Submitted;

/// Host-independent native cancellation failure domain.
pub trait Native {
    type Error<F>;
}

/// Native preparation failed without changing the requested operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause<F> {
    Absent(Absent),
    Reactor(F),
}

/// Queue exhaustion and rejected native state have distinct recovery paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<E> {
    Full(Full),
    Rejected(E),
}
