use std::{convert, pin};

use o3::buffer;

use crate::driver::{
    self, life,
    life::{
        domain::{Closing, Error, Rejected, Source, Teardown},
        recovery,
    },
    storage,
};

/// A single execution owns the driver and source and borrows its teardown owner.
pub struct Session<'a, Q> {
    pub(super) driver: driver::Driver,
    pub(super) source: Source<Q>,
    pub(super) teardown: &'a mut Teardown,
}

struct Scoped<F>(F);

impl<S, Q, F, R> life::Operation<S, Q> for Scoped<F>
where
    S: storage::Plan,
    F: for<'a, 'p, 'd> FnOnce(life::Scope<'a, 'd>, storage::Loan<'a, 'p, 'd, S>, &'a Q) -> R,
{
    type Output = R;

    fn run<'a, 'p, 'd>(
        self,
        scope: life::Scope<'a, 'd>,
        storage: storage::Loan<'a, 'p, 'd, S>,
        source: &'a Q,
    ) -> R
    where
        'd: 'p,
        'p: 'a,
        'd: 'a,
        Self: 'd,
        S: 'd,
        Q: 'd,
    {
        (self.0)(scope, storage, source)
    }
}

impl<'a, Q> Session<'a, Q> {
    /// Runs exact storage in the driver generation and reclaims it before teardown.
    pub fn scope<A, O>(
        self,
        plan: storage::Scope<A>,
        op: O,
        mut report: impl recovery::Observe,
    ) -> Result<O::Output, Error<'a, convert::Infallible, O::Output, Q>>
    where
        A: buffer::raw::Arenas,
        O: life::Operation<storage::Scope<A>, Q>,
    {
        self.run(|lease, source| Ok(lease.run_scope(plan.into_inner(), source, op, &mut report)))
    }

    /// Enters a generation and quiesces it before releasing storage or source.
    pub fn enter<S, O>(
        self,
        plan: S,
        op: O,
        mut report: impl recovery::Observe,
    ) -> Result<O::Output, Error<'a, convert::Infallible, O::Output, Q>>
    where
        S: storage::Direct,
        O: life::Operation<S, Q>,
    {
        self.run(|lease, source| Ok(lease.run_with(plan, source, op, &mut report)))
    }

    /// Runs a closure within the same generation and finalization as `enter`.
    pub fn enter_with<S, R>(
        self,
        plan: S,
        op: impl for<'s, 'p, 'd> FnOnce(life::Scope<'s, 'd>, storage::Loan<'s, 'p, 'd, S>, &'s Q) -> R,
        report: impl recovery::Observe,
    ) -> Result<R, Error<'a, convert::Infallible, R, Q>>
    where
        S: storage::Direct,
    {
        self.enter(plan, Scoped(op), report)
    }

    pub fn try_enter<S, O>(
        self,
        plan: S,
        op: O,
        mut report: impl recovery::Observe,
    ) -> Admission<'a, S::Error, O, O::Output, Q>
    where
        S: storage::Build,
        O: life::Operation<S, Q>,
    {
        self.run(|lease, source| lease.try_run_with(plan, source, op, &mut report))
    }
}

impl<'a, Q> Session<'a, Q> {
    pub fn try_create<S, O>(
        self,
        plan: S,
        op: O,
        mut report: impl recovery::Observe,
    ) -> Admission<'a, S::Error, O, O::Output, Q>
    where
        S: storage::Create,
        O: life::Operation<S, Q>,
    {
        self.run(|lease, source| lease.try_create(plan, source, op, &mut report))
    }

    fn run<E, T>(
        self,
        run: impl FnOnce(life::Lease<'_>, &Source<Q>) -> Result<T, E>,
    ) -> Result<T, Error<'a, E, T, Q>> {
        let Self {
            mut driver,
            source,
            teardown,
        } = self;
        let result = run(life::Lease::take(pin::Pin::new(&mut driver)), &source);
        Closing::close(result, driver.runtime, teardown, source)
    }
}

/// Preflight rejection retains the untouched operation beside the exact generation result.
type Admission<'a, E, O, T, Q> = Result<T, Error<'a, Rejected<E, O>, T, Q>>;
