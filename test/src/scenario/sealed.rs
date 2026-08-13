use std::{pin, task};

use dope::{
    core::driver::{
        self,
        lifecycle::{self, quiesce},
        retained, route,
    },
    runtime::executor::{self, session},
};

use crate::{checks::Outcome as _, dispatch};

pub(crate) struct Scope<'a>(pin::Pin<&'a mut driver::Driver>);

struct ReadyTurn;

impl<'d> executor::Root<'d> for ReadyTurn {
    type Output = ();

    fn poll(_context: executor::RootContext<'_, 'd, Self>) -> task::Poll<Self::Output> {
        task::Poll::Ready(())
    }
}

impl<'a> Scope<'a> {
    pub(crate) fn new(driver: pin::Pin<&'a mut driver::Driver>) -> Self {
        Self(driver)
    }

    pub(crate) fn enter<R>(self, f: impl for<'d> FnOnce(lifecycle::Scope<'d>) -> R) -> R {
        // SAFETY: the generative scope consumes every safe domain borrow before return.
        let owner = unsafe { quiesce::raw::Owner::new() };
        self.0.scope(quiesce::Lease::new(owner), f)
    }

    pub(crate) fn drain_ready<'scope, 'd: 'scope, S>(
        session: &mut session::Session<'scope, 'd, S>,
    ) -> Vec<route::Token> {
        let mut values = Vec::new();
        session
            .with_app(
                dispatch::ReadyCollector::probe::<0>(&mut values),
                |mut app| app.drive(ReadyTurn),
            )
            .or_abort("finish ready collector")
            .or_abort("drive ready collector");
        values
    }

    pub(crate) fn retained_context<'d>(
        context: driver::Context<'a, 'd>,
    ) -> retained::Context<'a, 'd, 'd> {
        // SAFETY: the driver owns this timer for the complete generative scope.
        let timer = unsafe { pin::Pin::new_unchecked(context.timer()) };
        // SAFETY: the timer remains pinned through final synchronous quiescence.
        let owner = unsafe { retained::raw::Owner::new(timer) };
        retained::Context::new(context, owner)
    }
}
