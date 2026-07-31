use std::hash::BuildHasher;
use std::process::abort;
use std::time::{Duration, Instant};

use super::{Action, DialEpoch, DialKey, Dialer};
use crate::hash::State;
use crate::runtime::__private::Deadline;
use dope_core::io::socket::addr::Addr;
use dope_net::Transport;
use o3::collections::IndexedMinHeap;
use o3::collections::SlotQueue;

#[derive(Clone, Copy)]
enum SlotState {
    Vacant,
    Ready,
    Busy { attempt: u8 },
    Backoff { attempt: u8 },
    Dead,
    Retired,
}

const MAX_BACKOFF_SHIFT: u8 = 10;

enum Target<A, O> {
    Static,
    Dynamic { addr: A, config: O },
}

struct StaticSlot<A, O> {
    target: Option<Target<A, O>>,
    state: SlotState,
    generation: DialEpoch,
}

impl<A, O> StaticSlot<A, O> {
    fn vacant() -> Self {
        Self {
            target: None,
            state: SlotState::Vacant,
            generation: DialEpoch::MIN,
        }
    }

    fn ready_static() -> Self {
        Self {
            target: Some(Target::Static),
            state: SlotState::Ready,
            generation: DialEpoch::MIN,
        }
    }

    fn is_dynamic(&self) -> bool {
        matches!(self.target, Some(Target::Dynamic { .. }))
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
    pub fn new(upstreams: Vec<T::Addr>, base_window: Duration, hash_builder: State) -> Self {
        let n = upstreams.len();
        let seed = hash_builder.hash_one((n, base_window.as_nanos()));
        let mut ready = SlotQueue::with_capacity(n);
        let free = SlotQueue::with_capacity(n);
        for index in 0..n {
            Self::push_back(&mut ready, index);
        }
        Self {
            upstreams,
            slots: (0..n).map(|_| StaticSlot::ready_static()).collect(),
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
        match self.slots.get(idx)?.target.as_ref()? {
            Target::Static => Some(&self.upstreams[idx % self.upstreams.len()]),
            Target::Dynamic { addr, .. } => Some(addr),
        }
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
        let delay = scaled / 2;
        let span = u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX).max(1);
        let jitter = Duration::from_nanos(r % span);
        Deadline::after(failed_at, delay.saturating_add(jitter))
    }

    fn matches(&self, key: DialKey) -> bool {
        self.slots.get(key.index() as usize).is_some_and(|slot| {
            matches!(
                slot.state,
                SlotState::Ready | SlotState::Busy { .. } | SlotState::Backoff { .. }
            ) && slot.generation == key.generation()
        })
    }

    fn advance_generation(&mut self, index: usize) -> bool {
        let slot = &mut self.slots[index];
        let Some(generation) = slot.generation.checked_add(1) else {
            slot.state = SlotState::Retired;
            return false;
        };
        slot.generation = generation;
        true
    }

    fn push_back(queue: &mut SlotQueue, index: usize) {
        Self::require(queue.push_back(index, ()).is_ok());
    }

    fn push_front(queue: &mut SlotQueue, index: usize) {
        Self::require(queue.push_front(index, ()).is_ok());
    }

    fn require(valid: bool) {
        if !valid {
            abort();
        }
    }

    fn schedule_ready(&mut self, index: usize) {
        Self::push_back(&mut self.ready, index);
        self.slots[index].state = SlotState::Ready;
    }

    fn schedule_backoff(&mut self, index: usize, attempt: u8, retry_at: Instant) {
        Self::require(self.retries.insert(index, retry_at).is_ok());
        self.slots[index].state = SlotState::Backoff { attempt };
    }

    fn schedule_dead(&mut self, index: usize) {
        if self.slots[index].is_dynamic() {
            Self::push_front(&mut self.free, index);
        }
        Self::push_back(&mut self.dead, index);
        self.slots[index].state = SlotState::Dead;
    }

    fn detach_active(&mut self, index: usize) -> bool {
        match self.slots[index].state {
            SlotState::Ready => {
                Self::require(self.ready.remove(index).is_some());
            }
            SlotState::Busy { .. } => {}
            SlotState::Backoff { .. } => {
                Self::require(self.retries.remove(index).is_some());
            }
            SlotState::Vacant | SlotState::Dead | SlotState::Retired => return false,
        }
        true
    }

    fn refresh_pending(&mut self) {
        self.needs_poll = !self.ready.is_empty() || !self.retries.is_empty();
    }
}

impl<T: Transport> Dialer<T> for Static<T> {
    fn resize(&mut self, max_connections: usize) {
        let want = max_connections.max(self.upstreams.len());
        if self.slots.len() < want {
            let first = self.slots.len();
            if self.upstreams.is_empty() {
                self.slots.resize_with(want, StaticSlot::vacant);
            } else {
                self.slots.resize_with(want, StaticSlot::ready_static);
            }
            self.ready.grow_to(want);
            self.free.grow_to(want);
            self.dead.grow_to(want);
            self.retries.grow_to(want);
            for index in first..want {
                if self.upstreams.is_empty() {
                    Self::push_back(&mut self.free, index);
                } else {
                    Self::push_back(&mut self.ready, index);
                }
            }
            self.needs_poll |= !self.upstreams.is_empty();
        }
    }

    fn dial(&mut self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        let (idx, ()) = self.free.pop_front_key_value()?;
        match self.slots[idx].state {
            SlotState::Vacant => {}
            SlotState::Dead if self.slots[idx].is_dynamic() => {
                Self::require(self.dead.remove(idx).is_some());
            }
            _ => abort(),
        }
        let slot = &mut self.slots[idx];
        let old_target = slot.target.replace(Target::Dynamic { addr, config });
        slot.state = SlotState::Ready;
        let key = DialKey::new(idx as u32, slot.generation);
        Self::push_back(&mut self.ready, idx);
        self.needs_poll = true;
        drop(old_target);
        Some(key)
    }

    fn poll_connect(&mut self, now: Instant) -> Action {
        if let Some((idx, ())) = self.ready.pop_front_key_value() {
            self.slots[idx].state = SlotState::Busy { attempt: 0 };
            self.refresh_pending();
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
        let SlotState::Backoff { attempt } = self.slots[idx].state else {
            abort();
        };
        self.slots[idx].state = SlotState::Busy { attempt };
        self.refresh_pending();
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
        if !self.matches(key) {
            return None;
        }
        match self.slots[key.index() as usize].target.as_ref()? {
            Target::Static => None,
            Target::Dynamic { config, .. } => Some(*config),
        }
    }

    fn connect_outcome(&mut self, key: DialKey, success: bool, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        let SlotState::Busy { attempt } = self.slots[idx].state else {
            return;
        };
        if success {
            self.slots[idx].state = SlotState::Busy { attempt: 0 };
            return;
        }
        let attempt = attempt.saturating_add(1).min(MAX_BACKOFF_SHIFT);
        if !self.advance_generation(idx) {
            self.refresh_pending();
            return;
        }
        let retry_at = self.retry_at(now, attempt);
        self.schedule_backoff(idx, attempt, retry_at);
        self.needs_poll = true;
    }

    fn connect_deferred(&mut self, key: DialKey, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if let SlotState::Busy { attempt } = self.slots[idx].state {
            if attempt == 0 {
                self.schedule_ready(idx);
            } else {
                self.schedule_backoff(idx, attempt, now);
            }
            self.needs_poll = true;
        }
    }

    fn disconnect(&mut self, key: DialKey, now: Instant) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if !self.detach_active(idx) {
            return;
        }
        if !self.advance_generation(idx) {
            self.refresh_pending();
            return;
        }
        if self.slots[idx].is_dynamic() {
            self.schedule_dead(idx);
            self.refresh_pending();
            return;
        }
        let retry_at = self.retry_at(now, 1);
        self.schedule_backoff(idx, 1, retry_at);
        self.needs_poll = true;
    }

    fn kill(&mut self, key: DialKey) {
        if !self.matches(key) {
            return;
        }
        let idx = key.index() as usize;
        if !self.detach_active(idx) {
            return;
        }
        if !self.advance_generation(idx) {
            self.refresh_pending();
            return;
        }
        self.schedule_dead(idx);
        self.refresh_pending();
    }

    fn revive(&mut self) {
        let mut revived = false;
        while let Some((idx, _)) = self.retries.pop() {
            if !matches!(self.slots[idx].state, SlotState::Backoff { .. }) {
                abort();
            }
            self.schedule_ready(idx);
            revived = true;
        }
        while let Some((idx, ())) = self.dead.pop_front_key_value() {
            if !matches!(self.slots[idx].state, SlotState::Dead) {
                abort();
            }
            if self.slots[idx].is_dynamic() {
                Self::require(self.free.remove(idx).is_some());
            }
            self.schedule_ready(idx);
            revived = true;
        }
        self.needs_poll |= revived;
    }
}
