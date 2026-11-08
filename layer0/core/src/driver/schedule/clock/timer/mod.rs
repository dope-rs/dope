use std::{mem, task, time};

use o3::mem::{Lease, Return, quota};

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
    fn due(
        &mut self,
        now: time::Instant,
    ) -> Option<heap::Minimum<'_, 'd, time::Instant, slot::Slot, Tag>> {
        let minimum = self.minimum()?;
        (*minimum.priority() <= now).then_some(minimum)
    }

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

    fn poll_alarm(
        &mut self,
        slot: &heap::Lease<'d, Tag>,
        now: time::Instant,
        target: &wake::Waker<'d>,
    ) -> task::Poll<()> {
        let state = self.state(slot);
        let due = state == heap::State::Fired || self.priority(slot).is_some_and(|due| *due <= now);
        if due {
            self.clear_alarm(slot);
            task::Poll::Ready(())
        } else if state == heap::State::Armed {
            self.set_target(slot, target);
            task::Poll::Pending
        } else {
            task::Poll::Pending
        }
    }
}

impl<'d> Timer<'d> {
    pub(in crate::driver) fn expire(self, ledger: &quota::Ledger<Tag>) {
        let now = self.driver.clock().now().into_raw();
        self.command(|mut cmd| {
            let Some(first) = cmd.due(now) else { return };
            let mut budget = quota::Lease::<Tag>::reserve_all(ledger);
            let mut next = budget.admit_with(|| Some(first));
            while let quota::Admission::Item(minimum) = next {
                minimum.fire().resident().wake_once(self.driver);
                next = budget.admit_with(|| cmd.due(now));
            }
        });
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
        self.lease
            .sink()
            .0
            .command(|cmd| cmd.state(self.lease.item()) != heap::State::Idle)
    }

    pub fn arm(&self, due: clock::Deadline<'d>, target: wake::Waker<'d>) {
        self.lease
            .sink()
            .0
            .command(|mut cmd| cmd.arm_alarm(self.lease.item(), due.into_raw(), &target));
    }

    /// Replaces this deadline and polls it in one command, retaining the slot.
    pub fn arm_and_poll(
        &self,
        due: clock::Deadline<'d>,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.lease.sink().0.command(|mut cmd| {
            if due <= now {
                cmd.clear_alarm(self.lease.item());
                task::Poll::Ready(())
            } else {
                cmd.arm_alarm(self.lease.item(), due.into_raw(), &target);
                task::Poll::Pending
            }
        })
    }

    /// Arms an idle alarm; otherwise polls without replacing its deadline.
    pub fn poll_or_arm(
        &self,
        due: clock::Deadline<'d>,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.lease.sink().0.command(|mut cmd| {
            let slot = self.lease.item();
            if cmd.state(slot) == heap::State::Idle {
                cmd.arm_alarm(slot, due.into_raw(), &target);
                task::Poll::Pending
            } else {
                cmd.poll_alarm(slot, now.into_raw(), &target)
            }
        })
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
            .command(|mut cmd| cmd.poll_alarm(self.lease.item(), now.into_raw(), &target))
    }

    pub fn clear(&self) {
        self.lease
            .sink()
            .0
            .command(|mut cmd| cmd.clear_alarm(self.lease.item()));
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
