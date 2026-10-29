use std::{marker, mem, os::fd, pin};

use dope_sys::memory::Context as _;
pub(in crate::driver) use epochs::Final;
use finish::Finish;
use o3::{
    base::{
        cell::{brand, region},
        marker::ThreadBound,
    },
    buffer,
    buffer::storage::{provided, provided::fixed},
};
use plans::{Build, Create, Direct, Provided};
use schedule::state::Core;
use state::scope;

use crate::{
    Engine, driver,
    driver::{
        life::recovery::{Observe, Recover},
        retained::flight,
        route::lease::Registry,
        schedule,
        schedule::state,
        storage,
        storage::{resident, resident::Alloc},
    },
    io,
};

pub mod creation;
pub mod domain;
pub mod finish;
pub(crate) mod native;
mod plans;
pub mod recovery;

/// Application completions still require ordinary driver dispatch before startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Busy;

/// Owner-bound primitives for generated installation roots.
pub mod raw;

/// Runs once inside the generation which owns a built storage output and source.
pub trait Operation<S: storage::Plan, Q: ?Sized>: Sized {
    type Output;

    fn run<'a, 'p, 'd>(
        self,
        scope: Scope<'a, 'd>,
        storage: storage::Loan<'a, 'p, 'd, S>,
        source: &'a Q,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'a,
        'd: 'a,
        Self: 'd,
        S: 'd,
        Q: 'd;
}

/// Mutable driver-local authority owned by one generative scope.
/// `driver::Context` lends this root as one pointer.
pub(crate) struct Local<'d> {
    pub(crate) driver: driver::Reference<'d>,
    pub(crate) backend: fixed::Loan<'d, Engine>,
    pub(in crate::driver) retry: bool,
    pub(crate) flights: &'d mut flight::Arena,
    pub(in crate::driver) resident: Alloc<'d>,
    pub(in crate::driver) routes: Registry,
    pub(in crate::driver) region: (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ),
    pub(crate) lifecycle: Lifecycle<'d>,
}

/// Generation-owned source and the policy responsible for its transitions.
pub(crate) struct Lifecycle<'d> {
    pub(crate) source: Option<fd::BorrowedFd<'d>>,
    pub(crate) resuming: bool,
    pub(crate) pending: bool,
    pub(crate) report: &'d mut dyn FnMut(io::Reclaimed),
    pub(in crate::driver) recover: &'d dyn recovery::Recover,
}

impl Lifecycle<'_> {
    pub(crate) fn report(&mut self, event: io::Reclaimed) {
        if matches!(event, io::Reclaimed::Shutdown) {
            let owned = self.source.take().is_some();
            if self.resuming && owned {
                self.pending = true;
                return;
            }
        }
        (self.report)(event);
    }
}

pub(in crate::driver) struct State<'d> {
    local: Local<'d>,
    work: Core<'d>,
    token: brand::Token<brand::Identity<'d>>,
}

impl<'d> State<'d> {
    fn new(local: Local<'d>, token: brand::Token<brand::Identity<'d>>) -> Self {
        Self {
            work: Core::new(local.driver),
            local,
            token,
        }
    }
}

mod epochs;

pub(in crate::driver) struct Lease<'a> {
    driver: pin::Pin<&'a mut driver::Driver>,
}

/// One lexical view of the hidden owner for an exact driver generation.
///
#[repr(transparent)]
pub struct Scope<'a, 'd> {
    state: &'a mut State<'d>,
}

impl<'a, 'd> Scope<'a, 'd> {
    fn new(state: &'a mut State<'d>) -> Self {
        Self { state }
    }

    pub fn context(&mut self) -> driver::Context<'_, 'd> {
        let state = &mut *self.state;
        driver::Context::new(&mut state.local)
    }

    pub fn token(&mut self) -> &mut brand::Token<brand::Identity<'d>> {
        &mut self.state.token
    }

    /// Runs with the sole schedule authority for this driver scope.
    pub fn with_turn<R>(
        &mut self,
        run: impl for<'scope> FnOnce(
            &'scope mut brand::Token<brand::Identity<'d>>,
            driver::Context<'scope, 'd>,
            scope::Scope<'scope, 'd>,
        ) -> R,
    ) -> R {
        let state = &mut *self.state;
        let ctx = driver::Context::new(&mut state.local);
        run(&mut state.token, ctx, scope::Scope::new(&mut state.work))
    }

    /// Runs a driver turn that performs no branded application projection.
    pub fn with_driver<R>(
        &mut self,
        run: impl for<'scope> FnOnce(driver::Context<'scope, 'd>, scope::Scope<'scope, 'd>) -> R,
    ) -> R {
        let state = &mut *self.state;
        let ctx = driver::Context::new(&mut state.local);
        run(ctx, scope::Scope::new(&mut state.work))
    }

    pub fn driver_ref(&self) -> driver::Reference<'d> {
        self.state.local.driver
    }

    /// Admits startup polling before any application completion can be emitted.
    /// Keep exclusive scope authority and do not submit operations during startup.
    pub fn try_boot(&self) -> Result<(), Busy> {
        if self.state.local.flights.is_pending() {
            Err(Busy)
        } else {
            Ok(())
        }
    }

    /// Drains every retained operation before captured owners may be released.
    /// A failed native transition retains its owner and invokes the fatal policy.
    /// Only successful native quiescence permits the borrowed scope to return.
    pub fn quiesce(&mut self) {
        let state = &mut *self.state;
        let ctx = driver::Context::new(&mut state.local);
        Final::suspend(ctx).resume()
    }

    /// Quiesces before lending finish authority, then reaps the released owner.
    /// A failed transition invokes the fatal policy with the exact native owner.
    #[doc(hidden)]
    pub fn finish<R>(
        &mut self,
        forced: bool,
        run: impl for<'finish> FnOnce(
            &mut brand::Token<brand::Identity<'d>>,
            &mut Finish<'finish, 'd>,
        ) -> R,
    ) -> R {
        let state = &mut *self.state;
        let ctx = driver::Context::new(&mut state.local);
        let last = Final::suspend(ctx);
        let mut finish = Finish::new(last, forced);
        let output = run(&mut state.token, &mut finish);
        finish.resume();
        output
    }
}

impl<'a> Lease<'a> {
    pub(crate) fn take(driver: pin::Pin<&'a mut driver::Driver>) -> Self {
        Self { driver }
    }

    fn run_with<S, Q: ?Sized, O>(
        self,
        plan: S,
        source: &domain::Source<Q>,
        op: O,
        report: &mut dyn Observe,
    ) -> O::Output
    where
        S: storage::Direct,
        O: Operation<S, Q>,
    {
        self.scope(
            source,
            Direct {
                plan,
                operation: op,
            },
            report,
        )
    }

    fn try_run_with<S, Q: ?Sized, O>(
        self,
        plan: S,
        source: &domain::Source<Q>,
        op: O,
        report: &mut dyn Observe,
    ) -> Result<O::Output, domain::Rejected<S::Error, O>>
    where
        S: storage::Build,
        O: Operation<S, Q>,
    {
        self.scope(
            source,
            Build {
                plan,
                operation: op,
            },
            report,
        )
    }

    fn try_create<S, Q: ?Sized, O>(
        self,
        plan: S,
        source: &domain::Source<Q>,
        op: O,
        report: &mut dyn Observe,
    ) -> Result<O::Output, domain::Rejected<S::Error, O>>
    where
        S: storage::Create,
        O: Operation<S, Q>,
    {
        self.scope(
            source,
            Create {
                plan,
                operation: op,
            },
            report,
        )
    }

    fn run_scope<A, Q: ?Sized, O>(
        self,
        arenas: A,
        source: &domain::Source<Q>,
        op: O,
        report: &mut dyn Observe,
    ) -> O::Output
    where
        A: buffer::raw::Arenas,
        O: Operation<storage::Scope<A>, Q>,
    {
        let runtime = &mut self.driver.get_mut().runtime;
        let (report, recover) = report.split();
        let root = Root {
            flights: &mut runtime.flights,
            resident: &mut runtime.resident,
            source,
            report,
            recover,
        };
        runtime.shared.run(
            pin::pin!((root, arenas)),
            runtime.shared.context().buffer().layout().0.accounts(),
            &mut runtime.backend,
            Provided(op),
        )
    }

    fn scope<'b, Q: ?Sized, O>(
        self,
        source: &'b domain::Source<Q>,
        op: O,
        report: &'b mut dyn Observe,
    ) -> O::Output
    where
        'a: 'b,
        O: fixed::Operation<Root<'b, Q>, driver::Buffer, driver::Shared, Terminal = crate::Engine>,
    {
        let runtime = &mut self.driver.get_mut().runtime;
        let (report, recover) = report.split();
        let root = Root {
            flights: &mut runtime.flights,
            resident: &mut runtime.resident,
            source,
            report,
            recover,
        };
        runtime.shared.run(
            pin::pin!(root),
            runtime.shared.context().buffer().layout().0.accounts(),
            &mut runtime.backend,
            op,
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Scope<'static, 'static>>() == mem::size_of::<&mut ()>());
    assert!(mem::align_of::<Scope<'static, 'static>>() == mem::align_of::<&mut ()>());
};

struct Root<'a, Q: ?Sized> {
    flights: &'a mut flight::Arena,
    resident: &'a mut resident::Arena,
    source: &'a domain::Source<Q>,
    report: &'a mut dyn FnMut(io::Reclaimed),
    recover: &'a dyn Recover,
}

impl<Q: ?Sized> Root<'_, Q> {
    fn enter<'d>(
        &'d mut self,
        shared: provided::Root<'d, driver::Buffer, driver::Shared, ()>,
        token: brand::Token<brand::Identity<'d>>,
        regions: (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        backend: fixed::Loan<'d, Engine>,
    ) -> (State<'d>, &'d Q) {
        let alloc = self.resident.enter(&regions.0);
        let local = Local {
            driver: driver::Reference::new(shared),
            backend,
            retry: false,
            flights: &mut *self.flights,
            resident: alloc,
            routes: Registry::new(),
            region: regions,
            lifecycle: Lifecycle {
                source: self.source.descriptor(),
                resuming: false,
                pending: false,
                report: &mut *self.report,
                recover: self.recover,
            },
        };
        let state = State::new(local, token);
        (state, &self.source.value)
    }
}

/// Proof that nested resources are installed beneath their pinned owner.
#[must_use]
pub struct Install<'a, 'd> {
    owner: marker::PhantomData<(&'a mut raw::Anchor<'a>, ThreadBound)>,
    _driver: driver::Invariant<'d>,
}

impl<'a, 'd> Install<'a, 'd> {
    /// Binds installation to `root` and its driver generation.
    pub fn new<T: raw::Root<'a>>(root: T, driver: driver::Reference<'d>) -> Self {
        Self {
            owner: root.anchor().owner(),
            _driver: driver.brand,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Install<'static, 'static>>() == 0);
    assert!(mem::align_of::<Install<'static, 'static>>() == 1);
};
