//! One absolute time budget backed by one reusable driver timer slot.

use core::{pin, task};
use std::{mem, time};

use crate::{
    abi::{self, machine},
    context,
    core::driver::schedule::clock::{self, timer},
};

/// Failure to construct an absolute budget in one driver time domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// The absolute deadline is outside the monotonic clock range.
    Range(clock::Range),
    /// The fixed timer store has no slot for this logical budget.
    Capacity(timer::Exhausted),
}

/// The bound that elapsed before an operation completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Elapsed {
    /// The enclosing absolute budget elapsed.
    Budget,
    /// A narrower operation limit elapsed.
    Limit,
}

/// One absolute multi-operation budget tied to an exact driver generation.
pub struct Budget<'d> {
    deadline: clock::Deadline<'d>,
    alarm: timer::Alarm<'d>,
}

/// One narrowed deadline borrowing its enclosing budget's timer slot.
pub struct Scope<'budget, 'd: 'budget> {
    deadline: clock::Deadline<'d>,
    alarm: &'budget mut timer::Alarm<'d>,
    elapsed: Elapsed,
}

/// A non-owning deadline check that leaves the enclosing timer available.
#[derive(Clone, Copy, Debug)]
pub struct Check<'d> {
    deadline: clock::Deadline<'d>,
    elapsed: Elapsed,
}

#[pin_project::pin_project(PinnedDrop)]
#[doc(hidden)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Timeout<'scope, 'd, F> {
    deadline: clock::Deadline<'d>,
    state: machine::Machine<State<'scope, 'd>>,
    limit: Option<time::Duration>,
    #[pin]
    fiber: F,
}

enum State<'scope, 'd> {
    Idle {
        alarm: &'scope mut timer::Alarm<'d>,
        elapsed: Elapsed,
    },
    Armed {
        alarm: timer::Armed<'scope, 'd>,
        elapsed: Elapsed,
    },
}

#[doc(hidden)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Sleep<'scope, 'd> {
    alarm: &'scope timer::Alarm<'d>,
    deadline: clock::Deadline<'d>,
    state: machine::Machine<SleepState>,
}

enum SleepState {
    New(time::Duration, Elapsed),
    Armed(Result<(), Elapsed>),
}

impl Elapsed {
    fn narrow<'d>(
        self,
        deadline: clock::Deadline<'d>,
        now: clock::Moment<'d>,
        duration: time::Duration,
    ) -> (clock::Deadline<'d>, Self) {
        let limit = now.try_add(duration).unwrap_or(deadline);
        if limit < deadline {
            (limit, Self::Limit)
        } else {
            (deadline, self)
        }
    }
}

impl Check<'_> {
    /// Checks fresh monotonic time without changing the driver's turn sample.
    pub fn check(self) -> Result<(), Elapsed> {
        if self.deadline.into_raw() <= time::Instant::now() {
            Err(self.elapsed)
        } else {
            Ok(())
        }
    }
}

impl<'d> Budget<'d> {
    /// Constructs one absolute budget and reserves its timer slot once.
    pub fn try_new(
        timer: timer::Timer<'d>,
        started: clock::Moment<'d>,
        duration: time::Duration,
    ) -> Result<Self, Error> {
        let deadline = started.try_add(duration).map_err(Error::Range)?;
        let alarm = timer.try_alarm().map_err(Error::Capacity)?;
        Ok(Self { deadline, alarm })
    }

    /// Samples a finite operation limit once without changing the driver clock.
    /// `Duration::MAX` keeps the enclosing bound without sampling.
    pub fn scope(&mut self, sample: clock::Moment<'d>, duration: time::Duration) -> Scope<'_, 'd> {
        if duration == time::Duration::MAX {
            return Scope {
                deadline: self.deadline,
                alarm: &mut self.alarm,
                elapsed: Elapsed::Budget,
            };
        }
        self.limit(sample.moment_at(time::Instant::now()), duration)
    }

    /// Narrows this budget without reserving another timer slot.
    pub fn limit(&mut self, now: clock::Moment<'d>, duration: time::Duration) -> Scope<'_, 'd> {
        let (deadline, elapsed) = Elapsed::Budget.narrow(self.deadline, now, duration);
        Scope {
            deadline,
            alarm: &mut self.alarm,
            elapsed,
        }
    }

    /// Runs one fiber under this budget's absolute deadline.
    pub fn timeout<'scope, F>(&'scope mut self, fiber: F) -> Timeout<'scope, 'd, F>
    where
        F: abi::Fiber<'d> + 'scope,
    {
        Timeout::new(&mut self.alarm, self.deadline, Elapsed::Budget, None, fiber)
    }

    /// Runs one fiber under the earlier of this budget and a relative limit.
    pub fn timeout_for<'scope, F>(
        &'scope mut self,
        duration: time::Duration,
        fiber: F,
    ) -> Timeout<'scope, 'd, F>
    where
        F: abi::Fiber<'d> + 'scope,
    {
        Timeout::new(
            &mut self.alarm,
            self.deadline,
            Elapsed::Budget,
            Some(duration),
            fiber,
        )
    }

    /// Waits without reserving another timer slot.
    pub fn sleep(&mut self, duration: time::Duration) -> Sleep<'_, 'd> {
        Sleep::new(&mut self.alarm, self.deadline, Elapsed::Budget, duration)
    }
}

impl<'budget, 'd: 'budget> Scope<'budget, 'd> {
    /// Copies this scope's selected deadline and exact elapsed cause.
    pub const fn checkpoint(&self) -> Check<'d> {
        Check {
            deadline: self.deadline,
            elapsed: self.elapsed,
        }
    }

    /// Runs one fiber under this scope's absolute deadline.
    pub fn timeout<'scope, F>(&'scope mut self, fiber: F) -> Timeout<'scope, 'd, F>
    where
        F: abi::Fiber<'d> + 'scope,
    {
        Timeout::new(&mut *self.alarm, self.deadline, self.elapsed, None, fiber)
    }

    /// Waits without reserving another timer slot.
    pub fn sleep(&mut self, duration: time::Duration) -> Sleep<'_, 'd> {
        Sleep::new(&mut *self.alarm, self.deadline, self.elapsed, duration)
    }
}

impl<'scope, 'd> Sleep<'scope, 'd> {
    fn new(
        alarm: &'scope mut timer::Alarm<'d>,
        deadline: clock::Deadline<'d>,
        elapsed: Elapsed,
        duration: time::Duration,
    ) -> Self {
        Self {
            alarm,
            deadline,
            state: machine::Machine::new(SleepState::New(duration, elapsed)),
        }
    }
}

impl<'d> abi::Fiber<'d> for Sleep<'_, 'd> {
    type Output = Result<(), Elapsed>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let now = context.as_mut().driver().driver_ref().clock().now();
        let alarm = this.alarm;
        let deadline = &this.deadline;
        this.state.turn(|state| {
            let wake = context.as_ref().completion();
            let (outcome, polled) = match *state {
                SleepState::New(duration, elapsed) => {
                    let limited = now.try_add(duration).unwrap_or(*deadline);
                    let (next, outcome) = if limited < *deadline {
                        (limited, Ok(()))
                    } else {
                        (*deadline, Err(elapsed))
                    };
                    *state = SleepState::Armed(outcome);
                    (outcome, alarm.arm_and_poll(next, now, wake))
                }
                SleepState::Armed(outcome) => (outcome, alarm.poll(now, wake)),
            };
            if polled.is_pending() {
                machine::Flow::Suspend(task::Poll::Pending)
            } else {
                machine::Flow::Complete(task::Poll::Ready(outcome))
            }
        })
    }
}

impl Drop for Sleep<'_, '_> {
    fn drop(&mut self) {
        if matches!(self.state.take(), Some(SleepState::Armed(_))) {
            self.alarm.clear();
        }
    }
}

impl<'scope, 'd, F> Timeout<'scope, 'd, F> {
    fn new(
        alarm: &'scope mut timer::Alarm<'d>,
        deadline: clock::Deadline<'d>,
        elapsed: Elapsed,
        limit: Option<time::Duration>,
        fiber: F,
    ) -> Self {
        Self {
            deadline,
            state: machine::Machine::new(State::Idle { alarm, elapsed }),
            limit,
            fiber,
        }
    }
}

impl<'d, F> abi::Fiber<'d> for Timeout<'_, 'd, F>
where
    F: abi::Fiber<'d>,
{
    type Output = Result<F::Output, Elapsed>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::Poll;

        let (this, mut context) = call.into_parts();
        let this = this.project();
        let deadline = this.deadline;
        let limit = this.limit;
        let fiber = this.fiber;
        this.state.step(|state| {
            let now = context.as_mut().driver().driver_ref().clock().now();
            let wake = context.as_ref().completion();
            let (polled, elapsed) = match state {
                State::Idle { alarm, mut elapsed } => {
                    if let Some(duration) = limit.take() {
                        (*deadline, elapsed) = elapsed.narrow(*deadline, now, duration);
                    }
                    (alarm.arm_owned(*deadline, now, wake), elapsed)
                }
                State::Armed { alarm, elapsed } => (alarm.poll(now, wake), elapsed),
            };
            match polled {
                timer::Poll::Ready => machine::Step::Complete(Poll::Ready(Err(elapsed))),
                timer::Poll::Pending(alarm) => match unsafe {
                    // SAFETY: this sole child receives the timeout's PollCall
                    // admission. The machine ends its polling authority on Ready.
                    context::raw::Live::new(fiber).delegate(context.as_mut())
                } {
                    Poll::Ready(output) => {
                        alarm.cancel();
                        machine::Step::Complete(Poll::Ready(Ok(output)))
                    }
                    Poll::Pending => {
                        machine::Step::Suspend(State::Armed { alarm, elapsed }, Poll::Pending)
                    }
                },
            }
        })
    }
}

#[pin_project::pinned_drop]
impl<F> PinnedDrop for Timeout<'_, '_, F> {
    fn drop(self: pin::Pin<&mut Self>) {
        if let Some(State::Armed { alarm, .. }) = self.project().state.take() {
            alarm.cancel();
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Error>() <= mem::size_of::<clock::Range>() + mem::size_of::<usize>());
    assert!(mem::size_of::<Elapsed>() == 1);
    assert!(mem::size_of::<State<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<machine::Machine<State<'static, 'static>>>()
            == mem::size_of::<State<'static, 'static>>()
    );
    assert!(
        mem::size_of::<Budget<'static>>()
            == mem::size_of::<time::Instant>() + mem::size_of::<timer::Alarm<'static>>()
    );
    assert!(mem::size_of::<Scope<'static, 'static>>() == 2 * mem::size_of::<time::Instant>());
};
