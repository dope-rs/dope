use std::convert::Infallible;

use dope_sys::{
    Errno,
    engine::{self, life},
    kernel,
    probe::Stopped,
};
use life::Close as _;

use crate::{
    Backend, driver,
    driver::{
        life::{Final, native::resolver::Resolver},
        result::{poll, poll::fault::Fault},
    },
    io,
};

/// Exact failure retained beside the attempt which still owns native retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    Completion(Fault),
    Reactor {
        cause: poll::Cause,
        completion: Option<Fault>,
    },
    File(poll::Cause),
    Tuning,
    TimerRefresh(Errno),
}

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Quiesce,
    Finish,
}

/// Exact failure and native owner which cannot leave the borrowed scope.
#[must_use]
pub struct Recovery<'a, 'd> {
    /// Retains the native owner throughout the failure policy.
    _context: driver::Context<'a, 'd>,
    cause: Cause,
}

/// One native transition retaining its exact driver borrow.
#[must_use]
pub(super) struct Attempt<'a, 'd> {
    context: driver::Context<'a, 'd>,
    phase: Phase,
}

/// Successful native retirement retaining its exact driver borrow.
///
/// An unsuccessful attempt cannot supply retirement authority.
///
#[must_use]
pub(super) struct Recovered<'a, 'd> {
    context: driver::Context<'a, 'd>,
}

impl<'a, 'd> Recovery<'a, 'd> {
    pub(in crate::driver) fn new(context: driver::Context<'a, 'd>, cause: Cause) -> Self {
        Self {
            _context: context,
            cause,
        }
    }

    pub fn cause(&self) -> &Cause {
        &self.cause
    }

    /// Aborts the process under the production panic=abort policy.
    /// Retains this recovery's native owner throughout termination.
    pub fn abort(self) -> ! {
        kernel::Fatal::Retirement.terminate()
    }

    pub(in crate::driver) fn terminate(self) -> ! {
        let recover = self._context.storage.0.lifecycle.recover;
        recover.recover(self)
    }
}

impl<'a, 'd> Attempt<'a, 'd> {
    pub(super) fn new(context: driver::Context<'a, 'd>, phase: Phase) -> Self {
        Self { context, phase }
    }

    pub(super) fn resume(self) -> Recovered<'a, 'd> {
        match self.try_resume() {
            Ok(done) => done,
            Err(failure) => failure.terminate(),
        }
    }

    pub(super) fn try_rearm(last: Final<'a, 'd>) -> Result<(), Recovery<'a, 'd>> {
        let resolver = Resolver::from(last);
        // SAFETY: the immutable generation source retains this exact descriptor
        // through the next native quiescence and teardown. Failed transitions
        // retain the same owner throughout the diverging recovery policy.
        let done = unsafe { <Backend as engine::raw::Resume<'_, _>>::try_resume(resolver) }
            .map_err(Self::stopped)?;
        done.local.lifecycle.resuming = false;
        Ok(())
    }

    /// Performs one native transition. Every failed attempt retains its owner.
    fn try_resume(self) -> Result<Recovered<'a, 'd>, Recovery<'a, 'd>> {
        let Self { context, phase } = self;
        let result = match phase {
            Phase::Quiesce => {
                <Backend as life::Quiesce<'_, _>>::try_quiesce(Resolver::from(context))
                    .map(life::Quiesced::into_inner)
            }
            Phase::Finish => {
                <Backend as life::Finish<'_, _>>::try_finish(Resolver::from(context).into_closing())
                    .map(life::Quiesced::into_inner)
            }
        };
        match result {
            Ok(resolver) => Ok(Recovered {
                context: resolver.into_context(),
            }),
            Err(failure) => Err(Self::stopped(failure)),
        }
    }

    fn stopped<State, E>(
        failure: Stopped<Resolver<'a, 'd, State>, life::Error<E, Fault>>,
    ) -> Recovery<'a, 'd>
    where
        poll::Cause: From<E>,
    {
        let Stopped { state, cause } = failure;
        let context = state.into_context();
        let cause: Cause = match cause {
            life::Error::Completion(fault) => Cause::Completion(fault),
            life::Error::Reactor {
                cause,
                completion: done,
            } => Cause::Reactor {
                cause: poll::Cause::from(cause),
                completion: done,
            },
            life::Error::File(cause) => Cause::File(poll::Cause::from(cause)),
            life::Error::Tuning => Cause::Tuning,
        };
        Recovery::new(context, cause)
    }
}

impl<'a, 'd> Recovered<'a, 'd> {
    pub(super) fn into_context(self) -> driver::Context<'a, 'd> {
        // Successful native quiescence drained completions and ended every
        // request borrow in this exact context; settle its remaining flights.
        self.context.storage.0.flights.finish();
        self.context
    }
}

/// Event reporting and the fatal native policy for one borrowed driver scope.
#[derive(Debug)]
pub struct Policy<R, F> {
    report: R,
    recover: F,
}

impl<R, F> Policy<R, F> {
    pub const fn new(report: R, recover: F) -> Self {
        Self { report, recover }
    }
}

/// Terminates an unrecoverable native transition while retaining its exact owner.
/// Every failed scope delegates here; no caller can resume after this boundary.
pub trait Recover {
    /// Consumes the exact native owner and classified failure without returning.
    /// The retained scope cannot resume; the return type requires divergence.
    fn recover<'a, 'd>(&self, failure: Recovery<'a, 'd>) -> !;
}

impl<F> Recover for F
where
    F: for<'a, 'd> Fn(Recovery<'a, 'd>) -> Infallible,
{
    fn recover<'a, 'd>(&self, failure: Recovery<'a, 'd>) -> ! {
        match self(failure) {}
    }
}

/// Separates mutable event reporting from the fatal policy's shared loan.
pub trait Observe {
    fn split(&mut self) -> (&mut dyn FnMut(io::Reclaimed), &dyn Recover);
}

impl<R: FnMut(io::Reclaimed), F: Recover> Observe for Policy<R, F> {
    fn split(&mut self) -> (&mut dyn FnMut(io::Reclaimed), &dyn Recover) {
        (&mut self.report, &self.recover)
    }
}
