use std::{cell, pin, rc, task, time};

use dope::runtime::executor::{self, session};
use dope_fiber::{abi, context, extensions::AppSessionExt as _, task::sleep, wait};

use crate::checks::Outcome as _;

pub struct Harness {
    guard: time::Duration,
}

impl Harness {
    pub const fn new(guard: time::Duration) -> Self {
        Self { guard }
    }

    pub fn drive<'app, 'd: 'app, D, F>(
        &self,
        app: &mut session::Application<'app, 'd, D>,
        fiber: F,
    ) -> F::Output
    where
        D: executor::Application<'d>,
        F: abi::Fiber<'d>,
    {
        app.block_on(fiber).or_abort("drive test fiber")
    }

    pub fn pause<'app, 'd: 'app, D>(
        &self,
        app: &mut session::Application<'app, 'd, D>,
        duration: time::Duration,
    ) where
        D: executor::Application<'d>,
    {
        self.drive(app, Delay::new(duration));
    }

    pub fn run_until<'target, 'app, 'd: 'app, D>(
        &self,
        app: &mut session::Application<'app, 'd, D>,
        gate: &'target Gate,
        want: u32,
    ) where
        D: executor::Application<'d>,
    {
        let reached = self.wait_until(app, gate, want);
        assert!(
            reached,
            "timed out after {:?}: gate took {} of {want} hits",
            self.guard,
            gate.hits()
        );
    }

    pub fn wait_until<'target, 'app, 'd: 'app, D>(
        &self,
        app: &mut session::Application<'app, 'd, D>,
        gate: &'target Gate,
        want: u32,
    ) -> bool
    where
        D: executor::Application<'d>,
    {
        let until = Until {
            gate,
            want,
            waiter: wait::Waiter::new(),
            timeout: Delay::new(self.guard),
        };
        app.block_on(until).or_abort("drive gate fiber")
    }

    /// Polls a fiber once, discards its output, and drops it before completing.
    pub fn cancel_after_poll<'d, F>(&self, fiber: F) -> impl abi::Fiber<'d, Output = ()>
    where
        F: abi::Fiber<'d>,
    {
        CancelAfterPoll { fiber: Some(fiber) }
    }
}

pub const TEST: Harness = Harness::new(crate::GUARD);

#[pin_project::pin_project]
struct CancelAfterPoll<F> {
    #[pin]
    fiber: Option<F>,
}

impl<'d, F> abi::Fiber<'d> for CancelAfterPoll<F>
where
    F: abi::Fiber<'d>,
{
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        if let Some(fiber) = this.fiber.as_mut().as_pin_mut()
            && cx.as_mut().try_poll(fiber).is_none()
        {
            return task::Poll::Pending;
        }
        this.fiber.set(None);
        task::Poll::Ready(())
    }
}

#[pin_project::pin_project(!Unpin)]
struct Delay<'d> {
    duration: time::Duration,
    #[pin]
    sleep: Option<sleep::Sleep<'d, 'd>>,
}

impl<'d> Delay<'d> {
    const fn new(duration: time::Duration) -> Self {
        Self {
            duration,
            sleep: None,
        }
    }
}

impl<'d> abi::Fiber<'d> for Delay<'d> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        if this.sleep.as_ref().get_ref().is_none() {
            let timer = cx.as_mut().driver_access().timer();
            let sleep = sleep::Sleep::new(timer, *this.duration).or_abort("test delay deadline");
            this.sleep.as_mut().set(Some(sleep));
        }
        cx.as_mut()
            .try_poll(
                this.sleep
                    .as_mut()
                    .as_pin_mut()
                    .or_abort("test delay initialized"),
            )
            .unwrap_or(task::Poll::Pending)
    }
}

#[pin_project::pin_project(!Unpin)]
struct GateState {
    hits: cell::Cell<u32>,
    #[pin]
    waiter: wait::Slot,
}

#[derive(Clone)]
pub struct Gate {
    state: pin::Pin<rc::Rc<GateState>>,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            state: rc::Rc::pin(GateState {
                hits: cell::Cell::new(0),
                waiter: wait::Slot::new(),
            }),
        }
    }
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hit(&self) {
        let state = self.state.as_ref();
        let state = state.project_ref();
        state.hits.set(state.hits.get().saturating_add(1));
        state.waiter.wake();
    }

    pub fn hits(&self) -> u32 {
        self.state.as_ref().project_ref().hits.get()
    }

    fn try_register<'target, 'd>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> bool {
        self.state
            .as_ref()
            .project_ref()
            .waiter
            .try_register(waiter, context)
    }
}

#[pin_project::pin_project(!Unpin)]
struct Until<'target, 'd> {
    gate: &'target Gate,
    want: u32,
    #[pin]
    waiter: wait::Waiter<'target, 'd>,
    #[pin]
    timeout: Delay<'d>,
}

impl<'target, 'd> abi::Fiber<'d> for Until<'target, 'd> {
    type Output = bool;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<bool> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let gate = *this.gate;
        if gate.hits() >= *this.want {
            return task::Poll::Ready(true);
        }

        let Some(timeout) = cx.as_mut().try_poll(this.timeout.as_mut()) else {
            return task::Poll::Pending;
        };
        if timeout.is_ready() {
            return task::Poll::Ready(false);
        }

        assert!(
            gate.try_register(this.waiter.as_ref(), cx.as_ref()),
            "gate already has an active waiter"
        );
        task::Poll::Pending
    }
}
