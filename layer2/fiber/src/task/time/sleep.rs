use std::{mem, pin, task, time};

use crate::{
    abi, context,
    core::driver::schedule::clock::{self, timer},
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
    state: State<T>,
}

#[derive(Clone, Copy)]
enum State<T> {
    New(T),
    Armed,
    Done,
}

#[doc(hidden)]
pub trait Start<'d>: Copy + Unpin {
    fn deadline(self, now: clock::Moment<'d>) -> clock::Deadline<'d>;
}

impl<'d> Start<'d> for time::Duration {
    fn deadline(self, now: clock::Moment<'d>) -> clock::Deadline<'d> {
        now.clamp_add(self)
    }
}

impl<'d> Start<'d> for clock::Deadline<'d> {
    fn deadline(self, _: clock::Moment<'d>) -> Self {
        self
    }
}

impl<'d> Sleep<'d> {
    pub fn new(timer: timer::Timer<'d>, duration: time::Duration) -> Result<Self, Error> {
        if duration > MAXIMUM_DURATION {
            return Err(Error::Duration { actual: duration });
        }
        Ok(Self {
            alarm: timer.try_alarm()?,
            state: State::New(duration),
        })
    }

    pub fn until(
        timer: timer::Timer<'d>,
        deadline: clock::Deadline<'d>,
    ) -> Result<Sleep<'d, clock::Deadline<'d>>, Error> {
        Ok(Sleep {
            alarm: timer.try_alarm()?,
            state: State::New(deadline),
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
        if let State::Done = self.state {
            return task::Poll::Pending;
        }
        let wake = context.as_ref().completion();
        let now = context.as_mut().driver().driver_ref().clock().now();
        let polled = if let State::New(start) = self.state {
            let polled = self.alarm.arm_and_poll(start.deadline(now), now, wake);
            self.state = State::Armed;
            polled
        } else {
            self.alarm.poll(now, wake)
        };
        if polled.is_ready() {
            self.state = State::Done;
        }
        polled
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
