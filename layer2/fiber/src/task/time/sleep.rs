use std::{mem, pin, task, time};

use crate::{
    abi, context,
    core::driver::schedule::{
        clock::{self, timer},
        ready::wake,
    },
};

pub const MAXIMUM_DURATION: time::Duration = time::Duration::new(i64::MAX as u64 / 2, 999_999_999);

pub trait TimerExt<'d> {
    fn sleep(&self, duration: time::Duration) -> Result<Sleep<'d>, Error>;
}

impl<'d> TimerExt<'d> for timer::Timer<'d> {
    fn sleep(&self, duration: time::Duration) -> Result<Sleep<'d>, Error> {
        Sleep::new(*self, duration)
    }
}

/// Sleep could not be represented or admitted to the fixed timer store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Error {
    Duration { actual: time::Duration },
    Capacity,
}

impl From<timer::Exhausted> for Error {
    fn from(_: timer::Exhausted) -> Self {
        Self::Capacity
    }
}

#[must_use = "a fiber does nothing unless it is driven"]
pub struct Sleep<'d, T = time::Duration> {
    alarm: timer::Alarm<'d>,
    state: abi::machine::Machine<T>,
}

#[doc(hidden)]
pub trait Start<'d>: Unpin {
    fn poll(
        &self,
        alarm: &timer::Alarm<'d>,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<()>;
}

impl<'d> Start<'d> for time::Duration {
    fn poll(
        &self,
        alarm: &timer::Alarm<'d>,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<()> {
        alarm.poll_after(self, now, wake)
    }
}

impl<'d> Start<'d> for clock::Deadline<'d> {
    fn poll(
        &self,
        alarm: &timer::Alarm<'d>,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<()> {
        alarm.poll_until(self, now, wake)
    }
}

impl<'d> Sleep<'d> {
    pub fn new(timer: timer::Timer<'d>, duration: time::Duration) -> Result<Self, Error> {
        if duration > MAXIMUM_DURATION {
            return Err(Error::Duration { actual: duration });
        }
        Ok(Self {
            alarm: timer.try_alarm()?,
            state: abi::machine::Machine::new(duration),
        })
    }

    pub fn until(
        timer: timer::Timer<'d>,
        deadline: clock::Deadline<'d>,
    ) -> Result<Sleep<'d, clock::Deadline<'d>>, Error> {
        Ok(Sleep {
            alarm: timer.try_alarm()?,
            state: abi::machine::Machine::new(deadline),
        })
    }
}

impl<'d, T: Start<'d>> Sleep<'d, T> {
    /// Polls this timer as a leaf of an already admitted application transition.
    /// Completion is retained here; subsequent polls remain pending.
    pub fn poll_sleep(
        &mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<()> {
        self.state.turn(|start| {
            let now = context.as_mut().driver().driver_ref().clock().now();
            start.poll(&self.alarm, now, context.as_ref().completion())
        })
    }
}

impl<'d, T: Start<'d>> abi::Fiber<'d> for Sleep<'d, T> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<()> {
        let (this, context) = call.into_parts();
        this.get_mut().poll_sleep(context)
    }
}

const _: () = assert!(mem::size_of::<Sleep<'static>>() == 4 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Error>() == 3 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Result<Sleep<'static>, Error>>() == 4 * mem::size_of::<usize>());
const _: () = {
    assert!(
        mem::size_of::<Sleep<'static, clock::Deadline<'static>>>() == 4 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Result<Sleep<'static, clock::Deadline<'static>>, Error>>()
            == 4 * mem::size_of::<usize>()
    );
};
