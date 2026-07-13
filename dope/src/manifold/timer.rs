use std::pin::Pin;
use std::time::Instant;

use crate::{Driver, backend};

pub trait HasTimer {
    fn timer(self: Pin<&mut Self>) -> Pin<&mut Timer<0>>;
}

const TIMER_CAP_HINT: usize = 4096;
const TIMER_DEFAULT_CAP: usize = 262144;
const NOT_IN_HEAP: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Ticket {
    slot: u32,
    epoch: u32,
}

enum State {
    Free,
    Armed(backend::park::WakeRef),
    Fired,
}

struct Slot {
    epoch: u32,
    heap_pos: u32,
    state: State,
}

struct HeapEntry {
    deadline: Instant,
    slot: u32,
    epoch: u32,
}

pub struct Timer<const ID: u8 = 0> {
    slots: Vec<Slot>,
    free: Vec<u32>,
    heap: Vec<HeapEntry>,
    starved: Vec<backend::park::WakeRef>,
    cap: usize,
}

impl<const ID: u8> Default for Timer<ID> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ID: u8> Timer<ID> {
    pub fn new() -> Self {
        Self::with_capacity(TIMER_DEFAULT_CAP)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let hint = TIMER_CAP_HINT.min(cap);
        Self {
            slots: Vec::with_capacity(hint),
            free: Vec::with_capacity(hint),
            heap: Vec::with_capacity(hint),
            starved: Vec::new(),
            cap,
        }
    }

    pub fn register_starved(&mut self, caller: backend::park::WakeRef) {
        if !self.starved.contains(&caller) {
            self.starved.push(caller);
        }
    }

    pub fn try_arm(&mut self, deadline: Instant, caller: backend::park::WakeRef) -> Option<Ticket> {
        let slot = match self.free.pop() {
            Some(s) => {
                self.slots[s as usize].state = State::Armed(caller);
                s
            }
            None => {
                if self.slots.len() >= self.cap {
                    return None;
                }
                let s = self.slots.len() as u32;
                self.slots.push(Slot {
                    epoch: 0,
                    heap_pos: NOT_IN_HEAP,
                    state: State::Armed(caller),
                });
                s
            }
        };
        let epoch = self.slots[slot as usize].epoch;
        self.heap_push(HeapEntry {
            deadline,
            slot,
            epoch,
        });
        Some(Ticket { slot, epoch })
    }

    pub fn is_fired(&self, ticket: Ticket) -> bool {
        match self.slots.get(ticket.slot as usize) {
            Some(s) => s.epoch == ticket.epoch && matches!(s.state, State::Fired),
            None => false,
        }
    }

    pub fn replace_caller(&mut self, ticket: Ticket, caller: backend::park::WakeRef) {
        if let Some(s) = self.slots.get_mut(ticket.slot as usize)
            && s.epoch == ticket.epoch
            && matches!(s.state, State::Armed(_))
        {
            s.state = State::Armed(caller);
        }
    }

    pub fn cancel(&mut self, ticket: Ticket) {
        let Some(s) = self.slots.get(ticket.slot as usize) else {
            return;
        };
        if matches!(s.state, State::Free) || s.epoch != ticket.epoch {
            return;
        }
        let pos = s.heap_pos;
        if pos != NOT_IN_HEAP {
            self.heap_remove(pos);
        }
        self.release(ticket.slot);
    }

    fn release(&mut self, slot: u32) {
        let s = &mut self.slots[slot as usize];
        s.state = State::Free;
        s.epoch = s.epoch.wrapping_add(1);
        s.heap_pos = NOT_IN_HEAP;
        self.free.push(slot);
        if !self.starved.is_empty() {
            for w in self.starved.drain(..) {
                w.wake();
            }
        }
    }

    pub fn earliest(&self) -> Option<Instant> {
        self.heap.first().map(|e| e.deadline)
    }

    pub fn expire(&mut self, now: Instant) {
        while let Some(top) = self.heap.first() {
            if top.deadline > now {
                break;
            }
            let Some(entry) = self.heap_pop() else { break };
            let Some(s) = self.slots.get_mut(entry.slot as usize) else {
                continue;
            };
            if s.epoch != entry.epoch {
                continue;
            }
            s.heap_pos = NOT_IN_HEAP;
            if let State::Armed(w) = std::mem::replace(&mut s.state, State::Fired) {
                w.wake();
            }
        }
    }

    fn heap_swap(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        let si = self.heap[i].slot as usize;
        let sj = self.heap[j].slot as usize;
        self.slots[si].heap_pos = i as u32;
        self.slots[sj].heap_pos = j as u32;
    }

    fn heap_push(&mut self, entry: HeapEntry) {
        let i = self.heap.len();
        let slot = entry.slot as usize;
        self.heap.push(entry);
        self.slots[slot].heap_pos = i as u32;
        self.sift_up(i);
    }

    fn heap_pop(&mut self) -> Option<HeapEntry> {
        let last = self.heap.len().checked_sub(1)?;
        self.heap_swap(0, last);
        let out = self.heap.pop()?;
        self.sift_down(0);
        Some(out)
    }

    fn heap_remove(&mut self, pos: u32) {
        let pos = pos as usize;
        let Some(last) = self.heap.len().checked_sub(1) else {
            return;
        };
        if pos >= self.heap.len() {
            return;
        }
        if pos == last {
            self.heap.pop();
            return;
        }
        self.heap_swap(pos, last);
        self.heap.pop();
        self.sift_down(pos);
        self.sift_up(pos);
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if self.heap[parent].deadline <= self.heap[i].deadline {
                break;
            }
            self.heap_swap(parent, i);
            i = parent;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let len = self.heap.len();
        loop {
            let l = 2 * i + 1;
            let r = 2 * i + 2;
            let mut smallest = i;
            if l < len && self.heap[l].deadline < self.heap[smallest].deadline {
                smallest = l;
            }
            if r < len && self.heap[r].deadline < self.heap[smallest].deadline {
                smallest = r;
            }
            if smallest == i {
                break;
            }
            self.heap_swap(i, smallest);
            i = smallest;
        }
    }
}

impl<const ID: u8> Timer<ID> {
    pub fn pre_park(self: Pin<&mut Self>, _driver: &Driver) {
        Pin::get_mut(self).expire(Instant::now());
    }

    pub fn idle(self: Pin<&Self>) -> crate::runtime::dispatcher::Idle {
        crate::runtime::dispatcher::Idle::Park(Pin::get_ref(self).earliest())
    }
}

impl<'d, const ID: u8> crate::manifold::Manifold<'d> for Timer<ID> {
    const ID: u8 = ID;

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver) {
        Timer::pre_park(self, driver)
    }

    fn idle(self: Pin<&Self>) -> crate::runtime::dispatcher::Idle {
        Timer::idle(self)
    }
}
