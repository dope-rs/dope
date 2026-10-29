mod error;
mod step;

use std::{ops::ControlFlow, os::fd::BorrowedFd};

pub use error::Error;
pub use step::Step;

use crate::{engine, probe::Stopped};

pub(super) type Retirement<T, R> = Result<
    T,
    Stopped<
        R,
        Error<<<R as Resolver>::Backend as engine::raw::Poll>::Error, <R as Resolver>::Fault>,
    >,
>;

/// A host's exact driver root retains all completion and retirement authority.
pub trait Resolver: Sized {
    type Backend: engine::raw::Poll;
    type Fault: Copy;

    fn backend(&mut self) -> &mut Self::Backend;
    fn retire_one(&mut self) -> Step;
    /// Retires every currently admissible descriptor, stopping at backpressure.
    fn retire(&mut self) {
        while let Step::Advanced = self.retire_one() {}
    }
    fn has_pending(&self) -> bool;
    fn has_flights(&self) -> bool;
    fn has_inflight(&self) -> bool;
}

/// A phase whose completions may only reclaim their registered owners.
pub trait Reclaim: Resolver {
    fn reclaim(self) -> Result<ControlFlow<Self, Self>, Stopped<Self, Self::Fault>>;
    /// Reports the exact completion fault once, returning its owner to drain.
    fn fault(failure: Stopped<Self, Self::Fault>) -> Self;
    /// Reports a native retirement failure while its retry owner remains retained.
    fn reactor(&mut self, failure: <Self::Backend as engine::raw::Poll>::Error);

    /// Consumes only the currently acquired completion batch. Every fault is
    /// reported before the first fault and the exact remaining owner return.
    fn drain(mut self) -> Result<Self, Stopped<Self, Self::Fault>> {
        let mut first = None;
        let state = loop {
            match self.reclaim() {
                Ok(ControlFlow::Continue(state)) => self = state,
                Ok(ControlFlow::Break(state)) => break state,
                Err(failure) => {
                    first.get_or_insert(failure.cause);
                    self = Self::fault(failure);
                }
            }
        };
        match first {
            None => Ok(state),
            Some(cause) => Err(Stopped { state, cause }),
        }
    }
}

/// The terminal host state borrowed alongside its exact native backend.
pub trait Terminal {
    fn has_flights(&self) -> bool;
    fn has_inflight(&self) -> bool;
    fn has_pending(&self) -> bool;
    fn source(&self) -> Option<BorrowedFd<'_>>;
}

/// Lends the disjoint backend and host state for final retirement and resumption.
pub trait Resume: Reclaim {
    type Terminal<'s>: Terminal
    where
        Self: 's;

    fn terminal(&mut self) -> (&mut Self::Backend, Self::Terminal<'_>);
}

/// Consumes an active phase before final descriptor retirement.
pub trait Close: Resolver {
    type Closing: Reclaim<Backend = Self::Backend, Fault = Self::Fault>;

    fn into_closing(self) -> Self::Closing;
}

/// Consumes running authority before canceling outstanding kernel operations.
pub trait Cancel: Close {
    type Cancelling: Reclaim<Backend = Self::Backend, Fault = Self::Fault>
        + Close<Closing = Self::Closing>;

    fn cancel(self) -> Self::Cancelling;
}

/// Native terminal census and retirement retain the exact resolved owner.
/// Implementations issue this authority only after the kernel no longer uses
/// that owner's borrowed request storage or pending retirement resources.
pub trait Quiesced {
    type Resolver: Reclaim;

    fn into_inner(self) -> Self::Resolver;
}

/// Native final retirement after the host has closed admission.
pub trait Finish<'a, R>
where
    R: Reclaim + 'a,
    <R::Backend as engine::raw::Poll>::Context: 'a,
{
    type Quiesced: Quiesced<Resolver = R>;

    fn try_finish(resolver: R) -> Retirement<Self::Quiesced, R>;
}

/// Native cancellation and final retirement consume the running host phase.
pub trait Quiesce<'a, R>
where
    R: Cancel + 'a,
    <R::Backend as engine::raw::Poll>::Context: 'a,
{
    type Quiesced: Quiesced<Resolver = R::Closing>;

    fn try_quiesce(resolver: R) -> Retirement<Self::Quiesced, R::Closing>;
}
