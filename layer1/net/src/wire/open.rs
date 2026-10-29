use dope_core::driver::{receive::credit, route::Identity};

use crate::wire::{self, failure};

/// Exact open error selected by the wire's failure class.
#[repr(transparent)]
pub struct Error<'d, W: wire::Spec + 'd>(
    <W::OpenFailure as failure::Class<W, failure::Open>>::Error<'d>,
);

impl<'d, W: wire::Spec + 'd> Error<'d, W> {
    pub const fn from_cause(
        cause: <W::OpenFailure as failure::Class<W, failure::Open>>::Error<'d>,
    ) -> Self {
        Self(cause)
    }

    pub const fn cause(&self) -> &<W::OpenFailure as failure::Class<W, failure::Open>>::Error<'d> {
        &self.0
    }

    pub fn into_cause(self) -> <W::OpenFailure as failure::Class<W, failure::Open>>::Error<'d> {
        self.0
    }
}

impl<'d, W> Error<'d, W>
where
    W: wire::Spec<OpenFailure = failure::Never> + 'd,
{
    /// Eliminates the uninhabited failure selected by `Never`.
    pub fn eliminate<T>(self) -> T {
        match self.0 {}
    }
}

/// Consumer-selected ownership result for one prepared wire open.
pub enum Disposition<T, W, S> {
    Commit(T),
    Retry { output: T, wire: W, send: S },
}

impl<T, W, S> Disposition<T, W, S> {
    pub fn retry(output: T, wire: W, send: S) -> Self {
        Self::Retry { output, wire, send }
    }

    pub fn into_output(self, retry: impl FnOnce(W, S)) -> T {
        match self {
            Self::Commit(output) => output,
            Self::Retry { output, wire, send } => {
                retry(wire, send);
                output
            }
        }
    }
}

/// Exact ownership result of attempting to acquire one prepared wire open.
pub enum Outcome<C, T, E> {
    Unavailable(C),
    Failed { context: C, cause: E },
    Opened(T),
}

/// State that opens one connection for its exact wire route.
pub trait Runtime<'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity + 'w> {
    /// Reports a resource transition that can resume a parked receive.
    fn is_ready(&self) -> bool {
        false
    }

    /// Takes one parked receive credit whose resource is ready.
    fn take_ready(&mut self) -> Option<credit::Held<'d, ID>> {
        None
    }

    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(
            C,
            W::Connection<'w, ID>,
            W::SendStorage<'w>,
        ) -> Disposition<T, W::Connection<'w, ID>, W::SendStorage<'w>>,
    ) -> Outcome<C, T, Error<'w, W>>;
}
