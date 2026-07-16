use std::cell::Cell;
use std::pin::Pin;
use std::time::Instant;

mod starved;

use starved::StarvedTree;
#[doc(hidden)]
pub use starved::Waiter;

use dope::manifold::Manifold;
use dope::runtime::Idle;
use o3::cell::RawCell;
use o3::collections::IndexedMinHeap;
use o3::marker::ThreadBound;

use dope::{DriverContext, DriverRef};
use dope_core::driver::ready::CompletionWaker;

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Ticket {
    slot: u32,
    epoch: u32,
    _thread: ThreadBound,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Free,
    Armed,
    Fired,
    Canceled,
    Retired,
}

struct Slot<'d> {
    epoch: Cell<u32>,
    state: Cell<State>,
    wake: Cell<Option<CompletionWaker<'d>>>,
    deadline: Cell<Instant>,
    next: Cell<u32>,
    _driver: std::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeapKey {
    deadline: Instant,
    epoch: u32,
}

pub struct Timer<'d, const ID: u8 = 0> {
    slots: Box<[Slot<'d>]>,
    free: Cell<u32>,
    pending_arm: Cell<u32>,
    pending_cancel: Cell<u32>,
    starved: StarvedTree<'d>,
    heap: RawCell<IndexedMinHeap<HeapKey>>,
}

#[derive(Clone, Copy)]
pub struct Session<'d, const ID: u8 = 0> {
    timer: &'d Timer<'d, ID>,
}

impl<'d, const ID: u8> Timer<'d, ID> {
    pub fn with_capacity(cap: usize, _driver: DriverRef<'d>) -> Self {
        assert!(cap <= u32::MAX as usize);
        let filler = Instant::now();
        Self {
            slots: (0..cap)
                .map(|index| Slot {
                    epoch: Cell::new(0),
                    state: Cell::new(State::Free),
                    wake: Cell::new(None),
                    deadline: Cell::new(filler),
                    next: Cell::new(if index == 0 { NIL } else { index as u32 - 1 }),
                    _driver: std::marker::PhantomData,
                })
                .collect(),
            free: Cell::new(if cap == 0 { NIL } else { cap as u32 - 1 }),
            pending_arm: Cell::new(NIL),
            pending_cancel: Cell::new(NIL),
            starved: StarvedTree::new(),
            heap: RawCell::new(IndexedMinHeap::with_capacity(cap)),
        }
    }

    pub fn session(&'d self) -> Session<'d, ID> {
        Session { timer: self }
    }

    #[doc(hidden)]
    pub fn register_starved(
        &self,
        waiter: Pin<&Waiter<'d>>,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) {
        self.starved.register(waiter, deadline, wake);
    }

    #[doc(hidden)]
    pub fn unregister_starved(&self, waiter: Pin<&Waiter<'d>>) {
        self.starved.unregister(waiter);
        if self.free.get() != NIL {
            self.starved.wake_min();
        }
    }

    pub fn try_arm(&self, deadline: Instant, wake: CompletionWaker<'d>) -> Option<Ticket> {
        let slot = self.free.get();
        if slot == NIL {
            return None;
        }
        let s = &self.slots[slot as usize];
        self.free.set(s.next.get());
        s.state.set(State::Armed);
        s.wake.set(Some(wake));
        s.deadline.set(deadline);
        s.next.set(self.pending_arm.get());
        self.pending_arm.set(slot);
        Some(Ticket {
            slot,
            epoch: s.epoch.get(),
            _thread: ThreadBound::NEW,
        })
    }

    pub fn is_fired(&self, ticket: Ticket) -> bool {
        match self.slots.get(ticket.slot as usize) {
            Some(s) => s.epoch.get() == ticket.epoch && s.state.get() == State::Fired,
            None => false,
        }
    }

    pub fn replace_waker(&self, ticket: Ticket, wake: CompletionWaker<'d>) {
        if let Some(s) = self.slots.get(ticket.slot as usize)
            && s.epoch.get() == ticket.epoch
            && s.state.get() == State::Armed
        {
            s.wake.set(Some(wake));
        }
    }

    pub fn cancel(&self, ticket: Ticket) {
        let Some(s) = self.slots.get(ticket.slot as usize) else {
            return;
        };
        if s.epoch.get() != ticket.epoch {
            return;
        }
        match s.state.get() {
            State::Fired => self.release(ticket.slot),
            State::Armed if !self.with_heap(|heap| heap.contains_key(ticket.slot as usize)) => {
                s.state.set(State::Canceled)
            }
            State::Armed => {
                s.state.set(State::Canceled);
                s.next.set(self.pending_cancel.get());
                self.pending_cancel.set(ticket.slot);
            }
            State::Free | State::Canceled | State::Retired => {}
        }
    }

    fn release(&self, slot: u32) {
        let s = &self.slots[slot as usize];
        s.wake.set(None);
        let Some(epoch) = s.epoch.get().checked_add(1) else {
            s.state.set(State::Retired);
            s.next.set(NIL);
            return;
        };
        s.epoch.set(epoch);
        s.state.set(State::Free);
        s.next.set(self.free.get());
        self.free.set(slot);
        self.starved.wake_min();
    }

    pub fn flush(&self) {
        let mut slot = self.pending_cancel.replace(NIL);
        while slot != NIL {
            let s = &self.slots[slot as usize];
            let next = s.next.get();
            if s.state.get() == State::Canceled {
                self.with_heap_mut(|heap| heap.remove(slot as usize));
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
                State::Armed => {
                    let key = HeapKey {
                        deadline: s.deadline.get(),
                        epoch: s.epoch.get(),
                    };
                    self.with_heap_mut(|heap| {
                        let Some(entry) = heap.vacant_entry(slot as usize) else {
                            unreachable!()
                        };
                        entry.insert(key);
                    });
                }
                State::Free | State::Fired | State::Retired => {}
            }
            slot = next;
        }
    }

    pub fn earliest(&self) -> Option<Instant> {
        let mut min = self.with_heap(|heap| heap.peek().map(|(_, key)| key.deadline));
        let mut slot = self.pending_arm.get();
        while slot != NIL {
            let s = &self.slots[slot as usize];
            if s.state.get() == State::Armed {
                let d = s.deadline.get();
                min = Some(min.map_or(d, |m| m.min(d)));
            }
            slot = s.next.get();
        }
        if let Some(deadline) = self.starved.min_deadline() {
            min = Some(min.map_or(deadline, |current| current.min(deadline)));
        }
        min
    }

    pub fn expire(&self, now: Instant) {
        self.starved.expire(now);
        self.flush();
        while self.with_heap(|heap| heap.peek().is_some_and(|(_, top)| top.deadline <= now)) {
            let Some((slot, key)) = self.with_heap_mut(IndexedMinHeap::pop) else {
                break;
            };
            let Some(s) = self.slots.get(slot) else {
                continue;
            };
            if s.epoch.get() != key.epoch || s.state.get() != State::Armed {
                continue;
            }
            s.state.set(State::Fired);
            if let Some(wake) = s.wake.get() {
                wake.wake();
            }
        }
    }

    fn with_heap<R>(&self, f: impl FnOnce(&IndexedMinHeap<HeapKey>) -> R) -> R {
        unsafe { self.heap.with(f) }
    }

    fn with_heap_mut<R>(&self, f: impl FnOnce(&mut IndexedMinHeap<HeapKey>) -> R) -> R {
        unsafe { self.heap.with_mut(f) }
    }
}

impl<const ID: u8> Timer<'_, ID> {
    pub fn pre_park(self: Pin<&Self>, driver: &mut DriverContext<'_, '_>) {
        Pin::get_ref(self).expire(driver.turn_now());
    }

    pub fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(Pin::get_ref(self).earliest())
    }
}

impl<'d, const ID: u8> Manifold<'d> for Session<'d, ID> {
    const ID: u8 = ID;

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref().get_ref().timer.expire(driver.turn_now());
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(self.get_ref().timer.earliest())
    }
}
