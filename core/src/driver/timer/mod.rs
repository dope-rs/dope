use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::time::Instant;

mod heap;
mod registration;
mod starved;

use heap::{Heap, Key};
use o3::cell::RegionToken;
use o3::marker::ThreadBound;
pub use registration::Registration;
use starved::Waiter;
use starved::{Entry, Queue};

use super::DriverRef;
use super::ready::{CompletionSlot, CompletionWaker};

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Ticket {
    slot: u32,
    epoch: u32,
    _thread: ThreadBound,
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

const _: () = assert!(size_of::<State>() == 1);

struct Slot<'d> {
    epoch: Cell<u32>,
    state: Cell<State>,
    wake: CompletionSlot<'d>,
    deadline: Cell<Instant>,
    next: Cell<u32>,
    _driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

pub struct Timer<'d> {
    slots: Box<[Slot<'d>]>,
    free: Cell<u32>,
    pending_arm: Cell<u32>,
    pending_cancel: Cell<u32>,
    waiters: Queue<'d>,
    heap: Heap<'d>,
}

impl<'d> Timer<'d> {
    pub(super) fn with_capacity(
        cap: usize,
        token: &RegionToken<'d>,
        _driver: DriverRef<'d>,
    ) -> Self {
        assert!(cap <= u32::MAX as usize);
        let filler = Instant::now();
        Self {
            slots: (0..cap)
                .map(|index| Slot {
                    epoch: Cell::new(0),
                    state: Cell::new(State::Free),
                    wake: CompletionSlot::empty(),
                    deadline: Cell::new(filler),
                    next: Cell::new(if index == 0 { NIL } else { index as u32 - 1 }),
                    _driver: PhantomData,
                })
                .collect(),
            free: Cell::new(if cap == 0 { NIL } else { cap as u32 - 1 }),
            pending_arm: Cell::new(NIL),
            pending_cancel: Cell::new(NIL),
            waiters: Queue::new(),
            heap: Heap::new(token, cap),
        }
    }

    fn waiter(&self) -> Waiter<'_, 'd> {
        Waiter::new(self)
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn register_waiter(
        &self,
        waiter: Pin<&Entry<'d>>,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) {
        self.waiters.register(waiter, deadline, wake);
    }

    fn unregister_waiter(&self, waiter: Pin<&Entry<'d>>) {
        if self.waiters.unregister(waiter) && self.free.get() != NIL {
            self.waiters.wake_min();
        }
    }

    fn try_arm(
        &self,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) -> Result<Ticket, CompletionWaker<'d>> {
        let slot = self.free.get();
        if slot == NIL {
            return Err(wake);
        }
        let s = &self.slots[slot as usize];
        self.free.set(s.next.get());
        s.state.set(State::Pending);
        s.wake.set(wake);
        s.deadline.set(deadline);
        s.next.set(self.pending_arm.get());
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

    fn replace_waker(&self, ticket: Ticket, wake: CompletionWaker<'d>) {
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
        self.waiters.wake_min();
    }

    pub fn flush(&self, token: &mut RegionToken<'d>) {
        let mut slot = self.pending_cancel.replace(NIL);
        while slot != NIL {
            let s = &self.slots[slot as usize];
            let next = s.next.get();
            if s.state.get() == State::Canceled {
                self.heap.remove(token, slot as usize);
                self.release(slot);
            }
            slot = next;
        }
        let mut slot = self.pending_arm.replace(NIL);
        while slot != NIL {
            let s = &self.slots[slot as usize];
            let next = s.next.get();
            match s.state.get() {
                State::Canceled => self.release(slot),
                State::Pending => {
                    let key = Key {
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
    }

    pub fn earliest(&self, token: &RegionToken<'d>) -> Option<Instant> {
        let mut min = self.heap.peek(token).map(|key| key.deadline);
        let mut slot = self.pending_arm.get();
        while slot != NIL {
            let s = &self.slots[slot as usize];
            if s.state.get() == State::Pending {
                let deadline = s.deadline.get();
                min = Some(min.map_or(deadline, |current| current.min(deadline)));
            }
            slot = s.next.get();
        }
        if let Some(deadline) = self.waiters.min_deadline() {
            min = Some(min.map_or(deadline, |current| current.min(deadline)));
        }
        min
    }

    pub fn expire(&self, token: &mut RegionToken<'d>, now: Instant) {
        self.waiters.expire(now);
        self.flush(token);
        while self.heap.peek(token).is_some_and(|top| top.deadline <= now) {
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
}
