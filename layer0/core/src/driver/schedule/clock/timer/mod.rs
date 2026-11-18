use std::{mem, task, time};

use o3::mem::{Lease, Return, lease, quota};

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
    lease: Lease<Access<'d>, lease::Open>,
}

impl<'d> Command<'_, 'd> {
    fn due(
        &mut self,
        now: time::Instant,
    ) -> Option<heap::Minimum<'_, 'd, time::Instant, slot::Slot, Tag>> {
        let minimum = self.minimum()?;
        (*minimum.priority() <= now).then_some(minimum)
    }

    fn arm_alarm(
        &mut self,
        slot: &heap::Lease<'d, Tag>,
        due: time::Instant,
        wake: &wake::Waker<'d>,
    ) {
        self.resident(slot).set(*wake.key());
        self.set(slot, due);
    }

    fn clear_alarm(&mut self, slot: &heap::Lease<'d, Tag>) {
        self.clear(slot);
        self.resident(slot).clear();
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
            lease: Lease::open(access, slot),
        }
    }

    /// Returns the timer and deadline state, releasing this resident slot.
    pub fn into_parts(self) -> (Timer<'d>, heap::State<clock::Deadline<'d>>) {
        let (Access(timer), slot) = self.lease.into_parts();
        let state = timer.command(|mut cmd| cmd.remove(slot).1);
        let state = match state {
            heap::State::Idle => heap::State::Idle,
            heap::State::Armed(due) => heap::State::Armed(timer.driver.clock().deadline_at(due)),
            heap::State::Fired => heap::State::Fired,
        };
        (timer, state)
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

    pub fn poll_after(
        &self,
        delay: &time::Duration,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.poll_with(now, target, || Some(now.clamp_add(delay).into_raw()))
    }

    pub fn poll_until(
        &self,
        due: &clock::Deadline<'d>,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
    ) -> task::Poll<()> {
        self.poll_with(now, target, || Some(due.into_raw()))
    }

    pub fn poll(&self, now: clock::Moment<'d>, target: wake::Waker<'d>) -> task::Poll<()> {
        self.poll_with(now, target, || None)
    }

    fn poll_with(
        &self,
        now: clock::Moment<'d>,
        target: wake::Waker<'d>,
        start: impl FnOnce() -> Option<time::Instant>,
    ) -> task::Poll<()> {
        self.lease.sink().0.command(|mut cmd| {
            cmd.poll_alarm(self.lease.item(), now.into_raw(), &target, start)
        })
    }

    pub fn clear(&self) {
        self.lease
            .sink()
            .0
            .command(|mut cmd| cmd.clear_alarm(self.lease.item()));
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
};
