use std::{task, time};

use dope_core::driver::{
    route::Identity,
    schedule::{
        clock::{self, timer},
        ready::{self, wake},
    },
};

use crate::client::conn::engine::drive::phase;

pub(in crate::client::conn::engine) struct Cycle<'d, ID: Identity, P> {
    backoff: timer::Alarm<'d>,
    retry: timer::Alarm<'d>,
    attempts: Attempts,
    phase: P,
    slot: ready::Slot<'d, ID>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::client) struct Attempts(u8);

impl Attempts {
    pub(in crate::client) const INITIAL: Self = Self(0);
    const MAX_SHIFT: u8 = 10;

    pub(in crate::client) const fn retry_millis(self) -> u64 {
        1_u64 << self.0 as u32
    }

    /// Advances this attempt count and returns the exact retry instant it schedules.
    pub(in crate::client) fn defer<'d>(&mut self, now: clock::Moment<'d>) -> clock::Deadline<'d> {
        let at = now.clamp_add(time::Duration::from_millis(self.retry_millis()));
        self.advance();
        at
    }

    pub(in crate::client) fn advance(&mut self) {
        if self.0 < Self::MAX_SHIFT {
            self.0 += 1;
        }
    }
}

impl<'d, ID: Identity, P: phase::Sequence> Cycle<'d, ID, P> {
    pub(in crate::client::conn::engine) fn advance(&mut self) -> P::Step {
        self.phase.advance()
    }
}

impl<'d, ID: Identity, P> Cycle<'d, ID, P> {
    pub(in crate::client::conn::engine) fn new(
        timer: timer::Timer<'d>,
        slot: ready::Slot<'d, ID>,
        phase: P,
    ) -> Result<Self, timer::Exhausted> {
        Ok(Self {
            backoff: timer.try_alarm()?,
            retry: timer.try_alarm()?,
            attempts: Attempts::INITIAL,
            phase,
            slot,
        })
    }

    pub(in crate::client::conn::engine) fn target(&self) -> ready::Target<'d> {
        self.slot.target()
    }

    pub(in crate::client::conn::engine) fn poll_backoff(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.backoff.poll(now, wake)
    }

    pub(in crate::client::conn::engine) fn arm_backoff(
        &mut self,
        deadline: clock::Deadline<'d>,
        wake: wake::Waker<'d>,
    ) {
        self.backoff.arm(deadline, wake);
    }

    pub(in crate::client::conn::engine) fn has_retry(&self) -> bool {
        self.retry.is_armed()
    }

    pub(in crate::client::conn::engine) fn poll_retry(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.retry.poll(now, wake)
    }

    pub(in crate::client::conn::engine) fn defer_retry(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) {
        let at = self.attempts.defer(now);
        self.retry.arm(at, wake);
    }

    pub(in crate::client::conn::engine) fn reset_retry(&mut self) {
        self.attempts = Attempts::INITIAL;
    }

    pub(in crate::client::conn::engine) fn stop(&mut self) {
        self.backoff.clear();
        self.retry.clear();
    }
}
