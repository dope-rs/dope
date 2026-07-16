use std::hash::BuildHasher;
use std::time::{Duration, Instant};

use super::{Action, DialEpoch, DialKey, Dialer};
use crate::hash;
use dope_core::driver::token::SlotIndex;
use dope_core::io::socket::addr::Addr;
use dope_net::Transport;
use o3::collections::IndexedMinHeap;
use o3::collections::SlotQueue;

#[derive(Clone, Copy, Default)]
enum Health {
    #[default]
    Idle,
    Busy {
        attempt: u8,
    },
    Backoff {
        attempt: u8,
    },
    Dead,
    Retired,
}

const MAX_BACKOFF_SHIFT: u8 = 10;

struct StaticSlot<A, O> {
    addr: Option<A>,
    config: Option<O>,
    health: Health,
    generation: DialEpoch,
    binding: Option<(DialEpoch, SlotIndex)>,
}

impl<A, O> StaticSlot<A, O> {
    fn new() -> Self {
        Self {
            addr: None,
            config: None,
            health: Health::Idle,
            generation: DialEpoch::MIN,
            binding: None,
        }
    }
}

pub struct Static<T: Transport> {
    upstreams: Vec<T::Addr>,
    slots: Vec<StaticSlot<T::Addr, T::StreamConfig>>,
    base_window: Duration,
    rng_state: u64,
    ready: SlotQueue,
    free: SlotQueue,
    dead: SlotQueue,
    retries: IndexedMinHeap<Instant>,
    needs_poll: bool,
}

impl<T: Transport> Static<T> {
    pub fn new(upstreams: Vec<T::Addr>, base_window: Duration, hash_builder: hash::State) -> Self {
        let n = upstreams.len();
        let seed = hash_builder.hash_one((n, base_window.as_nanos()));
        let mut ready = SlotQueue::with_capacity(n);
        let mut free = SlotQueue::with_capacity(n);
        for index in 0..n {
            if upstreams.is_empty() {
                let Some(entry) = free.vacant_entry(index) else {
                    unreachable!()
                };
                entry.push_back(());
            } else {
                let Some(entry) = ready.vacant_entry(index) else {
                    unreachable!()
                };
                entry.push_back(());
            }
        }
        Self {
            upstreams,
            slots: (0..n).map(|_| StaticSlot::new()).collect(),
            base_window,
            rng_state: seed.max(1),
            ready,
            free,
            dead: SlotQueue::with_capacity(n),
            retries: IndexedMinHeap::with_capacity(n),
            needs_poll: n != 0,
        }
    }

    fn addr_for(&self, idx: usize) -> Option<&T::Addr> {
        if let Some(a) = self.slots.get(idx)?.addr.as_ref() {
            return Some(a);
        }
        if self.upstreams.is_empty() {
            return None;
        }
        Some(&self.upstreams[idx % self.upstreams.len()])
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn retry_at(&mut self, failed_at: Instant, attempt: u8) -> Instant {
        let shift = attempt.min(MAX_BACKOFF_SHIFT) as u32;
        let scaled = self.base_window.saturating_mul(1u32 << shift);
        let r = self.next_rand();
        let span = (scaled.as_nanos() as u64 / 2).max(1);
        let jitter = Duration::from_nanos(r % span);
        failed_at + scaled / 2 + jitter
    }

    fn matches(&self, key: DialKey) -> bool {
        self.slots.get(key.index() as usize).is_some_and(|slot| {
            !matches!(slot.health, Health::Retired) && slot.generation == key.generation()
        })
    }

    fn advance_generation(&mut self, index: usize) -> bool {
        let slot = &mut self.slots[index];
        slot.binding = None;
        let Some(generation) = slot.generation.checked_add(1) else {
            slot.health = Health::Retired;
            return false;
        };
        slot.generation = generation;
        true
    }
}

impl<T: Transport> Dialer<T> for Static<T> {
    fn resize(&mut self, max_connections: usize) {
        let want = max_connections.max(self.upstreams.len());
        if self.slots.len() < want {
            let first = self.slots.len();
            self.slots.resize_with(want, StaticSlot::new);
            self.ready.grow_to(want);
            self.free.grow_to(want);
            self.dead.grow_to(want);
            self.retries.grow_to(want);
            for index in first..want {
                if self.upstreams.is_empty() {
                    let Some(entry) = self.free.vacant_entry(index) else {
                        unreachable!()
                    };
                    entry.push_back(());
                } else {
                    let Some(entry) = self.ready.vacant_entry(index) else {
                        unreachable!()
                    };
                    entry.push_back(());
                }
            }
            self.needs_poll |= !self.upstreams.is_empty();
        }
    }

    fn dial(&mut self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        let (idx, ()) = self.free.pop_front_key_value()?;
        self.dead.remove(idx);
        let slot = &mut self.slots[idx];
        slot.addr = Some(addr);
        slot.config = Some(config);
        slot.health = Health::Idle;
        let Some(entry) = self.ready.vacant_entry(idx) else {
            unreachable!()
        };
        entry.push_back(());
        self.needs_poll = true;
        Some(DialKey::new(idx as u32, slot.generation))
    }

    fn poll_connect(&mut self, now: Instant) -> Action {
        if let Some((idx, ())) = self.ready.pop_front_key_value() {
            self.slots[idx].health = Health::Busy { attempt: 0 };
            self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
            return Action::Connect {
                key: DialKey::new(idx as u32, self.slots[idx].generation),
            };
        }
        let Some((idx, &retry_at)) = self.retries.peek() else {
            self.needs_poll = false;
            return Action::Idle;
        };
        if retry_at > now {
            self.needs_poll = false;
            return Action::Backoff {
                min_retry_at: retry_at,
            };
        }
        self.retries.pop();
        let attempt = match self.slots[idx].health {
            Health::Backoff { attempt } => attempt,
            _ => unreachable!(),
        };
        self.slots[idx].health = Health::Busy { attempt };
        self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
        Action::Connect {
            key: DialKey::new(idx as u32, self.slots[idx].generation),
        }
    }

    fn has_pending(&self) -> bool {
        self.needs_poll
    }

    fn sock_addr(&self, key: DialKey) -> Option<Addr> {
        if !self.matches(key) {
            return None;
        }
        T::to_sock_addr(self.addr_for(key.index() as usize)?).ok()
    }

    fn socket_params(&self, key: DialKey) -> Option<(i32, i32, i32)> {
        if !self.matches(key) {
            return None;
        }
        Some(T::socket_params(self.addr_for(key.index() as usize)?))
    }

    fn stream_config(&self, key: DialKey) -> Option<T::StreamConfig> {
        self.matches(key)
            .then(|| self.slots[key.index() as usize].config)
            .flatten()
    }

    fn connect_outcome(&mut self, key: DialKey, success: bool, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if matches!(self.slots[idx].health, Health::Dead) {
            return;
        }
        if success {
            self.slots[idx].health = Health::Busy { attempt: 0 };
            return;
        }
        let prev_attempt = match self.slots[idx].health {
            Health::Busy { attempt } | Health::Backoff { attempt } => attempt,
            Health::Idle => 0,
            Health::Dead | Health::Retired => return,
        };
        let attempt = prev_attempt.saturating_add(1).min(MAX_BACKOFF_SHIFT);
        self.retries.remove(idx);
        if !self.advance_generation(idx) {
            self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
            return;
        }
        let retry_at = self.retry_at(now, attempt);
        self.slots[idx].health = Health::Backoff { attempt };
        let Some(entry) = self.retries.vacant_entry(idx) else {
            unreachable!()
        };
        entry.insert(retry_at);
        self.needs_poll = true;
    }

    fn connect_deferred(&mut self, key: DialKey, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if let Health::Busy { attempt } = self.slots[idx].health {
            if attempt == 0 {
                self.slots[idx].health = Health::Idle;
                let Some(entry) = self.ready.vacant_entry(idx) else {
                    unreachable!()
                };
                entry.push_back(());
            } else {
                self.slots[idx].health = Health::Backoff { attempt };
                let Some(entry) = self.retries.vacant_entry(idx) else {
                    unreachable!()
                };
                entry.insert(now);
            }
            self.needs_poll = true;
        }
    }

    fn disconnect(&mut self, key: DialKey, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if matches!(self.slots[idx].health, Health::Dead) {
            return;
        }
        self.ready.remove(idx);
        self.retries.remove(idx);
        if !self.advance_generation(idx) {
            self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
            return;
        }
        if self.slots[idx].addr.is_some() {
            self.slots[idx].health = Health::Dead;
            let Some(free) = self.free.vacant_entry(idx) else {
                unreachable!()
            };
            free.push_front(());
            let Some(dead) = self.dead.vacant_entry(idx) else {
                unreachable!()
            };
            dead.push_back(());
            return;
        }
        let retry_at = self.retry_at(now, 1);
        self.slots[idx].health = Health::Backoff { attempt: 1 };
        let Some(entry) = self.retries.vacant_entry(idx) else {
            unreachable!()
        };
        entry.insert(retry_at);
        self.needs_poll = true;
    }

    fn kill(&mut self, key: DialKey) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        self.ready.remove(idx);
        self.free.remove(idx);
        self.retries.remove(idx);
        if !self.advance_generation(idx) {
            self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
            return;
        }
        self.slots[idx].health = Health::Dead;
        if self.slots[idx].addr.is_some() {
            let Some(entry) = self.free.vacant_entry(idx) else {
                unreachable!()
            };
            entry.push_front(());
        }
        if !self.dead.contains_key(idx) {
            let Some(entry) = self.dead.vacant_entry(idx) else {
                unreachable!()
            };
            entry.push_back(());
        }
    }

    fn bind(&mut self, key: DialKey, local: SlotIndex) {
        if self.matches(key) {
            self.slots[key.index() as usize].binding = Some((key.generation(), local));
        }
    }

    fn revive(&mut self) {
        let mut revived = false;
        while let Some((idx, _)) = self.retries.pop() {
            self.slots[idx].health = Health::Idle;
            let Some(entry) = self.ready.vacant_entry(idx) else {
                unreachable!()
            };
            entry.push_back(());
            revived = true;
        }
        while let Some((idx, ())) = self.dead.pop_front_key_value() {
            self.free.remove(idx);
            self.slots[idx].health = Health::Idle;
            let Some(entry) = self.ready.vacant_entry(idx) else {
                unreachable!()
            };
            entry.push_back(());
            revived = true;
        }
        self.needs_poll |= revived;
    }
}
