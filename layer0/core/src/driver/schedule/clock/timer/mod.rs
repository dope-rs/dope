use std::{mem, task, time};

use o3::mem::{Lease, Return};

use crate::{
    driver,
    driver::schedule::{
        clock,
        ready::{wake, wake::resident::slot},
    },
};

pub mod heap;

pub(in crate::driver) struct Tag;

/// The driver's fixed timer capacity has no unleased resident slot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Exhausted {
    Slots,
}

/// Copyable access to the fixed timer store of one exact driver generation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Timer<'d> {
    pub(in crate::driver) driver: driver::Reference<'d>,
}

impl<'d> Timer<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self { driver }
    }

    pub fn earliest(self) -> Option<clock::Deadline<'d>> {
        self.command(|cmd| cmd.peek().copied())
            .map(|at| self.driver.clock().deadline_at(at))
    }

    pub fn try_alarm(self) -> Result<Alarm<'d>, Exhausted> {
        let slot = self
            .command(|mut cmd| {
                let vacancy = cmd.vacancy()?;
                Some(vacancy.lease(slot::Slot::default()))
            })
            .ok_or(Exhausted::Slots)?;
        Ok(Alarm::new(Access(self), slot))
    }
}

const _: () = {
    assert!(mem::size_of::<Timer<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Timer<'static>>() == mem::align_of::<usize>());
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Access<'d>(Timer<'d>);

/// Linear access to one timer store for the duration of one sealed command.
type Command<'turn, 'd> = heap::Min<'turn, 'd, time::Instant, slot::Slot, Tag>;

/// A resident alarm tied to one exact driver generation.
///
pub struct Alarm<'d> {
    lease: Lease<Access<'d>>,
}

/// Exclusive proof that one alarm remains armed or fired.
#[must_use = "an armed alarm must be polled or cancelled"]
#[repr(transparent)]
pub struct Armed<'a, 'd> {
    alarm: &'a mut Alarm<'d>,
}

/// State transition after polling an exclusively armed alarm.
#[must_use]
pub enum Poll<'a, 'd> {
    Pending(Armed<'a, 'd>),
    Ready,
}

impl<'d> Command<'_, 'd> {
    fn set_target(&mut self, slot: &heap::Lease<'d, Tag>, wake: &wake::Waker<'d>) {
        self.resident(slot).set(*wake.key());
    }

    fn arm_alarm(
        &mut self,
        slot: &heap::Lease<'d, Tag>,
        due: time::Instant,
        wake: &wake::Waker<'d>,
    ) {
        self.set_target(slot, wake);
        self.set(slot, due);
    }

    fn clear_alarm(&mut self, slot: &heap::Lease<'d, Tag>) {
        self.clear(slot);
        self.resident(slot).clear();
    }
}

impl<'d> Timer<'d> {
    fn has_alarm(self, slot: &heap::Lease<'d, Tag>) -> bool {
        self.command(|cmd| cmd.state(slot) != heap::State::Idle)
    }

    fn arm(self, slot: &heap::Lease<'d, Tag>, due: time::Instant, target: wake::Waker<'d>) {
        self.command(|mut cmd| cmd.arm_alarm(slot, due, &target));
    }

    fn arm_and_poll(
        self,
        slot: &heap::Lease<'d, Tag>,
        due: time::Instant,
        now: time::Instant,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.command(|mut cmd| {
            if due <= now {
                cmd.clear_alarm(slot);
                task::Poll::Ready(())
            } else {
                cmd.arm_alarm(slot, due, &target);
                task::Poll::Pending
            }
        })
    }

    fn poll(
        self,
        slot: &heap::Lease<'d, Tag>,
        now: time::Instant,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.command(|mut cmd| {
            let state = cmd.state(slot);
            let due =
                state == heap::State::Fired || cmd.priority(slot).is_some_and(|due| *due <= now);
            if due {
                cmd.clear_alarm(slot);
                task::Poll::Ready(())
            } else if state == heap::State::Armed {
                cmd.set_target(slot, &target);
                task::Poll::Pending
            } else {
                task::Poll::Pending
            }
        })
    }

    fn clear(self, slot: &heap::Lease<'d, Tag>) {
        self.command(|mut cmd| cmd.clear_alarm(slot));
    }

    pub(in crate::driver) fn expire_one(self, now: clock::Moment<'d>) -> Option<()> {
        self.command(|mut cmd| {
            let min = cmd.minimum()?;
            if *min.priority() > now.into_raw() {
                return None;
            }
            let fired = min.fire();
            fired.resident().wake_once(self.driver);
            Some(())
        })
    }

    fn command<T>(self, op: impl for<'turn> FnOnce(Command<'turn, 'd>) -> T) -> T {
        let shared = self.driver.shared;
        // SAFETY: this fixed owner cell supplies every `'d` lease; Command's
        // higher-ranked exclusive loan is linear, sealed, and non-reentrant.
        unsafe { shared.context().timer().loan(op) }
    }
}

impl<'d> Alarm<'d> {
    const fn new(access: Access<'d>, slot: heap::Lease<'d, Tag>) -> Self {
        Self {
            lease: Lease::new(access, slot),
        }
    }

    pub fn is_armed(&self) -> bool {
        self.lease.sink().0.has_alarm(self.slot())
    }

    pub fn arm(&self, due: clock::Deadline<'d>, target: wake::Waker<'d>) {
        self.lease.sink().0.arm(self.slot(), due.into_raw(), target);
    }

    /// Replaces this deadline and polls it in one command, retaining the slot.
    pub fn arm_and_poll(
        &self,
        due: clock::Deadline<'d>,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.lease
            .sink()
            .0
            .arm_and_poll(self.slot(), due.into_raw(), now.into_raw(), target)
    }

    /// Replaces and polls this deadline, retaining exclusive proof while pending.
    pub fn arm_owned<'a>(
        &'a mut self,
        due: clock::Deadline<'d>,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> Poll<'a, 'd> {
        if self.arm_and_poll(due, now, target).is_pending() {
            Poll::Pending(Armed { alarm: self })
        } else {
            Poll::Ready
        }
    }

    pub fn poll(&self, now: clock::Moment<'d>, target: wake::Waker<'d>) -> task::Poll<()> {
        self.lease
            .sink()
            .0
            .poll(self.slot(), now.into_raw(), target)
    }

    pub fn clear(&self) {
        self.lease.sink().0.clear(self.slot());
    }

    fn slot(&self) -> &heap::Lease<'d, Tag> {
        self.lease.item()
    }
}

impl<'a, 'd> Armed<'a, 'd> {
    /// Polls without losing the proof when the alarm remains live.
    pub fn poll(self, now: clock::Moment<'d>, target: wake::Waker<'d>) -> Poll<'a, 'd> {
        if self.alarm.poll(now, target).is_pending() {
            Poll::Pending(self)
        } else {
            Poll::Ready
        }
    }

    /// Cancels an alarm proven not to be idle.
    pub fn cancel(self) {
        self.alarm.clear();
    }
}

impl<'d> Return for Access<'d> {
    type Item = heap::Lease<'d, Tag>;

    fn return_item(self, slot: Self::Item) {
        self.0.command(|mut cmd| {
            cmd.release(slot);
        });
    }
}

const _: () = {
    assert!(mem::size_of::<heap::Lease<'static, Tag>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<heap::Lease<'static, Tag>>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Access<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Command<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Alarm<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Alarm<'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Armed<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Armed<'static, 'static>>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Poll<'static, 'static>>() == mem::size_of::<usize>());
};
