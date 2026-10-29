//! Native quiescence and retirement owned by the enclosing driver scope.

use std::{mem, pin::Pin};

use dope_sys::memory::buffer;
use o3::buffer::storage::provided::fixed;

use crate::{
    Engine, driver,
    driver::{
        life::{
            Local, Operation, Scope, State, domain,
            native::{drain, resolver::Reclaim},
            recovery::{Attempt, Phase, Recovered},
        },
        storage,
    },
    io,
};

/// Unforgeable witness that only this module can mint after native retirement.
struct Seal;

/// Native retirement has succeeded for the exact generation being ended.
pub(super) struct Closed {
    _seal: Seal,
}

impl fixed::Release<Engine, driver::Buffer> for Closed {
    fn release(&self, backend: &mut Engine, item: driver::Buffer) {
        // SAFETY: Closed is minted only after successful native retirement,
        // in the linear flow which finishes the generation's loan into this
        // End. End retains that loan and supplies every returned slot from
        // its storage, so the terminal is the exact owning engine.
        unsafe { buffer::raw::Release::release(backend, item) };
    }
}

#[must_use]
pub(in crate::driver) struct Final<'a, 'd> {
    context: driver::Context<'a, 'd>,
}

impl<'a, 'd> Final<'a, 'd> {
    pub(super) fn suspend(ctx: driver::Context<'a, 'd>) -> Self {
        ctx.storage.0.lifecycle.resuming = true;
        Self::start(ctx)
    }

    pub(super) fn start(mut ctx: driver::Context<'a, 'd>) -> Self {
        ctx.native().reclaim(usize::MAX);
        drain::Final::new(&mut ctx).all();
        let done = Attempt::new(ctx, Phase::Quiesce).resume();
        Self {
            context: done.into_context(),
        }
    }

    pub(in crate::driver::life) fn context(&self) -> &driver::Context<'a, 'd> {
        &self.context
    }

    pub(in crate::driver::life) fn context_mut(&mut self) -> &mut driver::Context<'a, 'd> {
        &mut self.context
    }

    /// Transfers the first completed native barrier's exact local loan.
    pub(in crate::driver::life) fn into_local(self) -> &'a mut Local<'d> {
        self.context.storage.0
    }

    fn drain(mut self) -> Self {
        self.context.native().reclaim(usize::MAX);
        drain::Final::new(&mut self.context).all();
        self
    }

    pub(in crate::driver::life) fn reap(self) -> Recovered<'a, 'd> {
        Attempt::new(self.drain().context, Phase::Finish).resume()
    }

    pub(super) fn resume(self) {
        if let Err(failure) = Attempt::try_rearm(self.drain()) {
            failure.terminate();
        }
    }
}

impl Reclaim for Final<'_, '_> {}

const _: () = {
    assert!(mem::size_of::<Closed>() == 0);
    assert!(
        mem::size_of::<fixed::End<'static, Engine, Closed>>()
            == mem::size_of::<fixed::Loan<'static, Engine>>()
    );
    assert!(mem::size_of::<Final<'static, 'static>>() == mem::size_of::<usize>());
};

impl Closed {
    fn retired(_done: Recovered<'_, '_>) -> Self {
        Self { _seal: Seal }
    }

    fn finish<'d>(mut state: State<'d>) -> (Self, fixed::Loan<'d, Engine>) {
        if mem::take(&mut state.local.lifecycle.pending) {
            (state.local.lifecycle.report)(io::Reclaimed::Shutdown);
        }
        let ctx = driver::Context::new(&mut state.local);
        let closed = Self::retired(Final::start(ctx).reap());
        (closed, state.local.backend)
    }

    pub(super) fn retire<'d>(state: State<'d>) -> fixed::End<'d, Engine, Closed> {
        let (closed, backend) = Self::finish(state);
        backend.finish(closed)
    }

    /// Retires a known failure while retaining the operation which has not run.
    pub(super) fn reject<'d, E, O, T>(
        state: State<'d>,
        cause: E,
        op: O,
    ) -> (
        Result<T, domain::Rejected<E, O>>,
        fixed::End<'d, Engine, Closed>,
    ) {
        let end = Self::retire(state);
        let error = domain::Rejected {
            cause,
            operation: op,
        };
        (Err(error), end)
    }

    /// Runs borrowed storage through native retirement before returning its loan.
    pub(super) fn run<'p, 'd: 'p, S, Q, O>(
        mut state: State<'d>,
        store: Pin<&S::Output<'p, 'd>>,
        source: &'d Q,
        op: O,
    ) -> (O::Output, fixed::End<'d, Engine, Closed>)
    where
        S: storage::Plan + 'd,
        Q: ?Sized + 'd,
        O: Operation<S, Q> + 'd,
    {
        let output = op.run(Scope::new(&mut state), storage::Loan::new(store), source);
        (output, Self::retire(state))
    }
}
