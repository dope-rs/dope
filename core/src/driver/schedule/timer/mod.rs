use std::{cell, cmp, marker, mem, num, time};

use o3::cell::region;

mod heap;
mod overflow;
mod registration;
pub use registration::Registration;

pub(super) enum Lane {}

use crate::driver::{
    self,
    schedule::{self, credits, ready::completion},
    settings,
};

const NIL: u32 = u32::MAX;

/// An absolute monotonic deadline confined to one driver lifetime and thread.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Deadline<'d> {
    at: time::Instant,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _thread: marker::PhantomData<*mut ()>,
}

const _: () = {
    assert!(mem::size_of::<Deadline<'static>>() == mem::size_of::<time::Instant>());
    assert!(mem::align_of::<Deadline<'static>>() == mem::align_of::<time::Instant>());
};

impl<'d> Deadline<'d> {
    pub(in crate::driver) const fn new(at: time::Instant) -> Self {
        Self {
            at,
            _driver: marker::PhantomData,
            _thread: marker::PhantomData,
        }
    }

    pub fn checked_add(self, duration: time::Duration) -> Option<Self> {
        self.at.checked_add(duration).map(Self::new)
    }
}

impl PartialOrd for Deadline<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Deadline<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at.cmp(&other.at)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Ticket {
    slot: u32,
    epoch: u32,
    _thread: o3::ThreadBound,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Free,
    Pending,
    Armed,
    Fired,
    Canceled,
    Retired,
}

#[derive(Clone, Copy)]
enum Phase {
    Overflow,
    Cancel,
    Arm,
    Heap,
}

impl Phase {
    const COUNT: usize = 4;

    const fn next(self) -> Self {
        match self {
            Self::Overflow => Self::Cancel,
            Self::Cancel => Self::Arm,
            Self::Arm => Self::Heap,
            Self::Heap => Self::Overflow,
        }
    }
}

const _: () = assert!(mem::size_of::<State>() == 1);

struct Slot<'d> {
    epoch: cell::Cell<u32>,
    state: cell::Cell<State>,
    wake: completion::Slot<'d>,
    deadline: cell::Cell<time::Instant>,
    pending_min: cell::Cell<time::Instant>,
    next: cell::Cell<u32>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

/// Address-stable timer storage shared by registrations in one driver scope.
///
/// ```compile_fail
/// use dope_core::driver::schedule::timer::Timer;
///
/// fn require_unpin<T: Unpin>() {}
/// require_unpin::<Timer<'static>>();
/// ```
pub struct Timer<'d> {
    slots: Box<[Slot<'d>]>,
    free: cell::Cell<u32>,
    pending_arm: cell::Cell<u32>,
    pending_cancel: cell::Cell<u32>,
    phase: cell::Cell<Phase>,
    overflow: overflow::Overflow<'d>,
    heap: heap::Heap<'d>,
    _pin: marker::PhantomPinned,
}

impl<'d> Timer<'d> {
    pub fn deadline_after(&self, duration: time::Duration) -> Option<Deadline<'d>> {
        time::Instant::now()
            .checked_add(duration)
            .map(Deadline::new)
    }

    pub(in crate::driver) fn with_capacity(
        limit: settings::ScheduleCapacity,
        token: &region::Token<'d>,
    ) -> Self {
        let mut capacity = num::NonZeroUsize::new(limit.get());
        while let Some(current) = capacity {
            use o3::collections::BoxSliceExt;

            let cap = current.get();
            let filler = time::Instant::now();
            let slots = BoxSliceExt::try_box_with(cap, |index| Slot {
                epoch: cell::Cell::new(0),
                state: cell::Cell::new(State::Free),
                wake: completion::Slot::empty(),
                deadline: cell::Cell::new(filler),
                pending_min: cell::Cell::new(filler),
                next: cell::Cell::new(if index == 0 { NIL } else { index as u32 - 1 }),
                _driver: marker::PhantomData,
            });
            if let Ok(slots) = slots
                && let Ok(heap) = heap::Heap::try_new(token, current)
            {
                return Self {
                    slots,
                    free: cell::Cell::new(cap as u32 - 1),
                    pending_arm: cell::Cell::new(NIL),
                    pending_cancel: cell::Cell::new(NIL),
                    phase: cell::Cell::new(Phase::Overflow),
                    overflow: overflow::Overflow::new(),
                    heap,
                    _pin: marker::PhantomPinned,
                };
            }
            capacity = num::NonZeroUsize::new(cap / 2);
        }
        Self {
            slots: Box::default(),
            free: cell::Cell::new(NIL),
            pending_arm: cell::Cell::new(NIL),
            pending_cancel: cell::Cell::new(NIL),
            phase: cell::Cell::new(Phase::Overflow),
            overflow: overflow::Overflow::new(),
            heap: heap::Heap::empty(token),
            _pin: marker::PhantomPinned,
        }
    }

    fn waiter(&self) -> overflow::Waiter<'_, 'd> {
        use crate::driver::schedule::timer::overflow::Waiter;
        Waiter::new(self)
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn has_free_slot(&self) -> bool {
        self.free.get() != NIL
    }

    fn try_arm(
        &self,
        deadline: time::Instant,
        wake: completion::Waker<'d>,
    ) -> Result<Ticket, completion::Waker<'d>> {
        use o3::ThreadBound;
        let slot = self.free.get();
        if slot == NIL {
            return Err(wake);
        }
        let s = &self.slots[slot as usize];
        self.free.set(s.next.get());
        s.state.set(State::Pending);
        s.wake.set(wake);
        s.deadline.set(deadline);
        let next = self.pending_arm.get();
        let pending_min = if next == NIL {
            deadline
        } else {
            deadline.min(self.slots[next as usize].pending_min.get())
        };
        s.pending_min.set(pending_min);
        s.next.set(next);
        self.pending_arm.set(slot);
        Ok(Ticket {
            slot,
            epoch: s.epoch.get(),
            _thread: ThreadBound::NEW,
        })
    }

    fn is_fired(&self, ticket: Ticket) -> bool {
        match self.slots.get(ticket.slot as usize) {
            Some(s) => s.epoch.get() == ticket.epoch && s.state.get() == State::Fired,
            None => false,
        }
    }

    fn replace_waker(&self, ticket: Ticket, wake: completion::Waker<'d>) {
        if let Some(s) = self.slots.get(ticket.slot as usize)
            && s.epoch.get() == ticket.epoch
            && matches!(s.state.get(), State::Pending | State::Armed)
        {
            s.wake.set(wake);
        }
    }

    fn cancel(&self, ticket: Ticket) -> bool {
        let Some(s) = self.slots.get(ticket.slot as usize) else {
            return false;
        };
        if s.epoch.get() != ticket.epoch {
            return false;
        }
        match s.state.get() {
            State::Fired => self.release(ticket.slot),
            State::Pending => s.state.set(State::Canceled),
            State::Armed => {
                s.state.set(State::Canceled);
                s.next.set(self.pending_cancel.get());
                self.pending_cancel.set(ticket.slot);
            }
            State::Free | State::Canceled | State::Retired => return false,
        }
        true
    }

    fn release(&self, slot: u32) {
        let s = &self.slots[slot as usize];
        s.wake.clear();
        let Some(epoch) = s.epoch.get().checked_add(1) else {
            s.state.set(State::Retired);
            s.next.set(NIL);
            return;
        };
        s.epoch.set(epoch);
        s.state.set(State::Free);
        s.next.set(self.free.get());
        self.free.set(slot);
        self.overflow.wake_min();
    }

    fn flush_cancel(
        &self,
        token: &mut region::Token<'d>,
        budget: &mut credits::Budget<'_, 'd, Lane>,
    ) {
        let mut slot = self.pending_cancel.replace(NIL);
        while slot != NIL && budget.take() {
            let s = &self.slots[slot as usize];
            let next = s.next.get();
            if s.state.get() == State::Canceled {
                self.heap.remove(token, slot as usize);
                self.release(slot);
            }
            slot = next;
        }
        if slot != NIL {
            self.pending_cancel.set(slot);
        }
    }

    fn flush_arm(&self, token: &mut region::Token<'d>, budget: &mut credits::Budget<'_, 'd, Lane>) {
        let mut slot = self.pending_arm.replace(NIL);
        while slot != NIL && budget.take() {
            let s = &self.slots[slot as usize];
            let next = s.next.get();
            match s.state.get() {
                State::Canceled => self.release(slot),
                State::Pending => {
                    let key = heap::Key {
                        deadline: s.deadline.get(),
                        epoch: s.epoch.get(),
                    };
                    self.heap.insert(token, slot as usize, key);
                    s.state.set(State::Armed);
                }
                State::Free | State::Armed | State::Fired | State::Retired => {}
            }
            slot = next;
        }
        if slot != NIL {
            self.pending_arm.set(slot);
        }
    }

    fn expire_heap(
        &self,
        token: &mut region::Token<'d>,
        now: time::Instant,
        budget: &mut credits::Budget<'_, 'd, Lane>,
    ) {
        while self.heap.peek(token).is_some_and(|top| top.deadline <= now) {
            if !budget.take() {
                break;
            }
            let Some((slot, key)) = self.heap.pop(token) else {
                break;
            };
            let Some(s) = self.slots.get(slot) else {
                continue;
            };
            if s.epoch.get() != key.epoch || s.state.get() != State::Armed {
                continue;
            }
            s.state.set(State::Fired);
            s.wake.wake();
        }
    }

    pub fn earliest(&self, token: &region::Token<'d>) -> Option<time::Instant> {
        let mut min = self.heap.peek(token).map(|key| key.deadline);
        let pending = self.pending_arm.get();
        if pending != NIL {
            let deadline = self.slots[pending as usize].pending_min.get();
            min = Some(min.map_or(deadline, |current| current.min(deadline)));
        }
        if let Some(deadline) = self.overflow.min_deadline() {
            min = Some(min.map_or(deadline, |current| current.min(deadline)));
        }
        min
    }

    pub fn expire(
        &self,
        work: schedule::Timers<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        now: time::Instant,
    ) {
        self.expire_with(driver.region_token(), now, work);
    }

    fn expire_with(
        &self,
        token: &mut region::Token<'d>,
        now: time::Instant,
        work: schedule::Timers<'_, 'd>,
    ) {
        let mut budget = credits::Budget::from_timers(work);
        let mut phase = self.phase.get();
        for _ in 0..Phase::COUNT {
            self.phase.set(phase.next());
            if budget.remaining() == 0 {
                return;
            }
            match phase {
                Phase::Overflow => self.overflow.expire(now, &mut budget),
                Phase::Cancel => self.flush_cancel(token, &mut budget),
                Phase::Arm => self.flush_arm(token, &mut budget),
                Phase::Heap => self.expire_heap(token, now, &mut budget),
            }
            phase = phase.next();
        }
    }
}
