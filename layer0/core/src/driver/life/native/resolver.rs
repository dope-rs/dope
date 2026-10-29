use std::{marker, mem, ops::ControlFlow, os::fd, task};

use dope_sys::{engine, engine::life, probe::Stopped, table::Retire};

use crate::{
    Engine, driver,
    driver::{
        life::{Final, Lifecycle, Local, native::retired::Retired},
        receive::tokens,
        result::poll::{self, fault},
        retained::flight,
    },
    io::{self, fd::domain::retire, outputs},
};

/// Completion authority borrowed from one exact driver-local root.
/// Consuming zero-sized phases make terminal progress linear; the `Local`
/// borrow keeps backend, flight-arena, and driver identities inseparable.
#[repr(transparent)]
pub(crate) struct Resolver<'a, 'd, State = Running> {
    pub(crate) local: &'a mut Local<'d>,
    state: marker::PhantomData<State>,
}

pub(crate) struct Running;
pub(crate) struct Cancelling;
pub(crate) struct Closing;

/// Final census and source borrowed separately from the exact native backend.
pub(crate) struct Terminal<'s, 'd> {
    descriptors: retire::View<'d>,
    flights: &'s flight::Arena,
    lifecycle: &'s Lifecycle<'d>,
}

pub(crate) trait Reclaim {}

impl<'a, 'd> Resolver<'a, 'd, Running> {
    pub(super) fn new(local: &'a mut Local<'d>) -> Self {
        Self {
            local,
            state: marker::PhantomData,
        }
    }
}

impl<'a, 'd> From<driver::Context<'a, 'd>> for Resolver<'a, 'd, Running> {
    fn from(context: driver::Context<'a, 'd>) -> Self {
        Self::new(context.storage.0)
    }
}

impl<'a, 'd> From<Final<'a, 'd>> for Resolver<'a, 'd, Final<'a, 'd>> {
    fn from(last: Final<'a, 'd>) -> Self {
        Self {
            local: last.into_local(),
            state: marker::PhantomData,
        }
    }
}

impl<'a, 'd, State> Resolver<'a, 'd, State> {
    pub(in crate::driver) fn into_context(self) -> driver::Context<'a, 'd> {
        driver::Context::new(self.local)
    }

    /// Consumes the head selected from this local domain without intervening mutation.
    pub(super) fn release(&mut self, pending: retire::Pending<'d>) -> life::Step {
        let retire = self.local.driver.descriptors().retirement();
        match Retire::retire(&mut *self.local.backend, pending.classify(), retire) {
            task::Poll::Ready(done) => {
                match Retired::from(done) {
                    Retired::Submitted => {}
                    Retired::Closed(report) => (self.local.lifecycle.report)(report),
                }
                life::Step::Advanced
            }
            task::Poll::Pending => life::Step::Blocked,
        }
    }
}

impl<'a, 'd> life::Cancel for Resolver<'a, 'd, Running> {
    type Cancelling = Resolver<'a, 'd, Cancelling>;

    fn cancel(self) -> Self::Cancelling {
        Resolver {
            local: self.local,
            state: marker::PhantomData,
        }
    }
}

impl<'a, 'd> life::Close for Resolver<'a, 'd, Running> {
    type Closing = Resolver<'a, 'd, Closing>;

    fn into_closing(self) -> Self::Closing {
        Resolver {
            local: self.local,
            state: marker::PhantomData,
        }
    }
}

impl<'a, 'd> life::Close for Resolver<'a, 'd, Cancelling> {
    type Closing = Resolver<'a, 'd, Closing>;

    fn into_closing(self) -> Self::Closing {
        Resolver {
            local: self.local,
            state: marker::PhantomData,
        }
    }
}

impl<State> life::Resolver for Resolver<'_, '_, State> {
    type Backend = Engine;
    type Fault = fault::Fault;

    fn backend(&mut self) -> &mut Self::Backend {
        &mut self.local.backend
    }

    fn retire_one(&mut self) -> life::Step {
        let Some(pending) = self.local.driver.descriptors().retirement().pending() else {
            return life::Step::Empty;
        };
        self.release(pending)
    }

    fn has_pending(&self) -> bool {
        self.local.driver.descriptors().retirement().has_pending()
    }

    fn has_flights(&self) -> bool {
        self.local.flights.is_pending()
    }

    fn has_inflight(&self) -> bool {
        self.local.driver.descriptors().retirement().has_inflight()
    }
}

impl<'a, 'd> life::Resume for Resolver<'a, 'd, Final<'a, 'd>> {
    type Terminal<'s>
        = Terminal<'s, 'd>
    where
        Self: 's;

    fn terminal(&mut self) -> (&mut Engine, Self::Terminal<'_>) {
        let Local {
            driver,
            backend,
            flights,
            lifecycle: state,
            ..
        } = &mut *self.local;
        (
            &mut **backend,
            Terminal {
                descriptors: driver.descriptors().retirement(),
                flights,
                lifecycle: state,
            },
        )
    }
}

impl life::Terminal for Terminal<'_, '_> {
    fn has_flights(&self) -> bool {
        self.flights.is_pending()
    }

    fn has_inflight(&self) -> bool {
        self.descriptors.has_inflight()
    }

    fn has_pending(&self) -> bool {
        self.descriptors.has_pending()
    }

    fn source(&self) -> Option<fd::BorrowedFd<'_>> {
        self.lifecycle.source
    }
}

impl<'a, 'd> Resolver<'a, 'd, Running> {
    pub(crate) fn resolve(mut self) -> Option<Result<Option<io::Event<'d>>, fault::Fault>> {
        if mem::take(&mut self.local.lifecycle.pending) {
            return Some(Ok(Some(outputs::shutdown())));
        }
        let Some(entry) = tokens::Token::next(&mut *self.local)? else {
            return Some(Ok(None));
        };
        Some(match crate::completion::resolve(&mut self, entry) {
            Ok(event) => Ok(event.map(|event| {
                let kind = event.into_kind();
                if matches!(kind, io::event::Kind::Shutdown) {
                    self.local.lifecycle.source = None;
                }
                kind.into()
            })),
            Err(Stopped { state, cause }) => {
                if let Some(buffer) = state {
                    buffer.release(self.local);
                }
                Err(cause)
            }
        })
    }
}

impl<'a, 'd, State: Reclaim> life::Reclaim for Resolver<'a, 'd, State> {
    fn reclaim(mut self) -> Result<ControlFlow<Self, Self>, Stopped<Self, fault::Fault>> {
        let entry = match tokens::Token::next(&mut *self.local) {
            Some(Some(entry)) => entry,
            Some(None) => return Ok(ControlFlow::Continue(self)),
            None => return Ok(ControlFlow::Break(self)),
        };
        match crate::completion::reclaim(&mut self, entry) {
            Ok(()) => Ok(ControlFlow::Continue(self)),
            Err(Stopped { state, cause }) => {
                if let Some(buffer) = state {
                    buffer.release(self.local);
                }
                Err(Stopped { state: self, cause })
            }
        }
    }

    fn fault(failure: Stopped<Self, fault::Fault>) -> Self {
        (failure.state.local.lifecycle.report)(io::Reclaimed::Fault(failure.cause));
        failure.state
    }

    fn reactor(&mut self, failure: <Engine as engine::raw::Poll>::Error) {
        (self.local.lifecycle.report)(io::Reclaimed::Reactor(poll::Cause::from(failure)));
    }
}

impl Reclaim for Cancelling {}

impl Reclaim for Closing {}

const _: () = {
    assert!(mem::size_of::<Resolver<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Resolver<'static, 'static, Cancelling>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Resolver<'static, 'static, Closing>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Resolver<'static, 'static, Final<'static, 'static>>>()
            == mem::size_of::<usize>()
    );
};
