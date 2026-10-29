use std::mem;

mod context;

pub use context::Context;

/// Exact reason a startup root owns the current poll.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Cause {
    Start,
    Wake,
}

/// Consuming startup transition that cannot be polled after completion.
#[must_use]
pub enum Poll<R, T> {
    Pending(R),
    Ready(T),
}

/// Startup control flow with no I/O submission or arbitrary ready-slot authority.
pub trait Root<'d>: Sized {
    type Output;
    fn poll(self, context: Context<'_, 'd>) -> Poll<Self, Self::Output>;
    /// Releases this root when startup admission or execution fails.
    fn cancel(self);
}

const _: () = assert!(mem::size_of::<Cause>() == mem::size_of::<u8>());
