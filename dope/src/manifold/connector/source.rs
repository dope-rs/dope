use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::transport::Transport;
use crate::{backend, socket};

pub enum Action<T: Transport> {
    Connect { addr: T::Addr, tag: u32 },
    Backoff { min_retry_at: Instant },
    Idle,
}

pub trait Dialer<T: Transport> {
    fn resize(&mut self, max_conn: usize);
    fn dial(&mut self, addr: T::Addr) -> Option<u32> {
        let _ = addr;
        None
    }
    fn poll_connect(&mut self, now: Instant) -> Action<T>;
    fn sock_addr(&self, tag: u32) -> Option<socket::Addr>;
    fn connect_outcome(&mut self, tag: u32, success: bool, now: Instant);
    fn connect_deferred(&mut self, tag: u32, now: Instant) {
        let _ = (tag, now);
    }
    fn disconnect(&mut self, tag: u32, now: Instant);
    fn kill(&mut self, tag: u32);
    fn revive(&mut self) {}
}

pub struct Explicit<T: Transport> {
    slots: Vec<Option<T::Addr>>,
    pending: VecDeque<u32>,
}

impl<T: Transport> Default for Explicit<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            pending: VecDeque::new(),
        }
    }
}

impl<T: Transport> Dialer<T> for Explicit<T>
where
    T::Addr: Clone,
{
    fn resize(&mut self, max_conn: usize) {
        if self.slots.len() < max_conn {
            self.slots.resize_with(max_conn, || None);
        }
    }

    fn dial(&mut self, addr: T::Addr) -> Option<u32> {
        let tag = match self.slots.iter().position(Option::is_none) {
            Some(i) => i,
            None => {
                self.slots.push(None);
                self.slots.len() - 1
            }
        };
        self.slots[tag] = Some(addr);
        self.pending.push_back(tag as u32);
        Some(tag as u32)
    }

    fn poll_connect(&mut self, _now: Instant) -> Action<T> {
        while let Some(tag) = self.pending.pop_front() {
            if let Some(Some(addr)) = self.slots.get(tag as usize) {
                return Action::Connect {
                    addr: addr.clone(),
                    tag,
                };
            }
        }
        Action::Idle
    }

    fn sock_addr(&self, tag: u32) -> Option<backend::socket::Addr> {
        let addr = self.slots.get(tag as usize)?.clone()?;
        T::to_sock_addr(addr).ok()
    }

    fn connect_outcome(&mut self, tag: u32, success: bool, _now: Instant) {
        if !success && let Some(s) = self.slots.get_mut(tag as usize) {
            *s = None;
        }
    }

    fn disconnect(&mut self, tag: u32, _now: Instant) {
        if let Some(s) = self.slots.get_mut(tag as usize) {
            *s = None;
        }
    }

    fn kill(&mut self, tag: u32) {
        if let Some(s) = self.slots.get_mut(tag as usize) {
            *s = None;
        }
    }
}

#[derive(Clone, Copy, Default)]
enum Health {
    #[default]
    Idle,
    Busy {
        attempt: u8,
    },
    Backoff {
        retry_at: Instant,
        attempt: u8,
    },
    Dead,
}

const MAX_BACKOFF_SHIFT: u8 = 10;

pub struct Static<T: Transport> {
    upstreams: Vec<T::Addr>,
    overrides: Vec<Option<T::Addr>>,
    states: Vec<Health>,
    base_window: Duration,
    next: u32,
    rng_state: u64,
}

impl<T: Transport> Static<T>
where
    T::Addr: Clone,
{
    pub fn new(upstreams: Vec<T::Addr>, base_window: Duration) -> Self {
        let n = upstreams.len();
        let entropy = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let seed =
            entropy ^ (n as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ base_window.as_nanos() as u64;
        Self {
            upstreams,
            overrides: vec![None; n],
            states: vec![Health::default(); n],
            base_window,
            next: 0,
            rng_state: seed.max(1),
        }
    }

    fn addr_for(&self, idx: usize) -> Option<T::Addr> {
        if let Some(Some(a)) = self.overrides.get(idx) {
            return Some(a.clone());
        }
        if self.upstreams.is_empty() {
            return None;
        }
        Some(self.upstreams[idx % self.upstreams.len()].clone())
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
        let jitter = std::time::Duration::from_nanos(r % span);
        failed_at + scaled / 2 + jitter
    }
}

impl<T: Transport> Dialer<T> for Static<T>
where
    T::Addr: Clone,
{
    fn resize(&mut self, max_conn: usize) {
        let want = max_conn.max(self.upstreams.len());
        if self.states.len() < want {
            self.states.resize(want, Health::default());
            self.overrides.resize(want, None);
        }
    }

    fn dial(&mut self, addr: T::Addr) -> Option<u32> {
        let reuse = self
            .states
            .iter()
            .enumerate()
            .position(|(i, s)| matches!(s, Health::Dead) && self.overrides[i].is_some());
        let idx = match reuse {
            Some(i) => i,
            None => {
                self.states.push(Health::Idle);
                self.overrides.push(None);
                self.states.len() - 1
            }
        };
        self.overrides[idx] = Some(addr);
        self.states[idx] = Health::Idle;
        Some(idx as u32)
    }

    fn poll_connect(&mut self, now: Instant) -> Action<T> {
        if self.states.is_empty() {
            return Action::Idle;
        }
        let n = self.states.len() as u32;
        let mut min_retry: Option<Instant> = None;
        for offset in 0..n {
            let idx = ((self.next + offset) % n) as usize;
            let attempt = match self.states[idx] {
                Health::Idle => 0,
                Health::Backoff { retry_at, attempt } => {
                    if retry_at > now {
                        min_retry = Some(min_retry.map_or(retry_at, |p| p.min(retry_at)));
                        continue;
                    }
                    attempt
                }
                Health::Busy { .. } | Health::Dead => continue,
            };
            let Some(addr) = self.addr_for(idx) else {
                continue;
            };
            self.next = (idx as u32 + 1) % n;
            self.states[idx] = Health::Busy { attempt };
            return Action::Connect {
                addr,
                tag: idx as u32,
            };
        }
        match min_retry {
            Some(min_retry_at) => Action::Backoff { min_retry_at },
            None => Action::Idle,
        }
    }

    fn sock_addr(&self, tag: u32) -> Option<backend::socket::Addr> {
        let addr = self.addr_for(tag as usize)?;
        T::to_sock_addr(addr).ok()
    }

    fn connect_outcome(&mut self, tag: u32, success: bool, now: Instant) {
        let idx = tag as usize;
        if idx >= self.states.len() {
            return;
        }
        if matches!(self.states[idx], Health::Dead) {
            return;
        }
        if success {
            self.states[idx] = Health::Busy { attempt: 0 };
            return;
        }
        let prev_attempt = match self.states[idx] {
            Health::Busy { attempt } | Health::Backoff { attempt, .. } => attempt,
            Health::Idle => 0,
            Health::Dead => return,
        };
        let attempt = prev_attempt.saturating_add(1).min(MAX_BACKOFF_SHIFT);
        let retry_at = self.retry_at(now, attempt);
        self.states[idx] = Health::Backoff { retry_at, attempt };
    }

    fn connect_deferred(&mut self, tag: u32, now: Instant) {
        let idx = tag as usize;
        if idx >= self.states.len() {
            return;
        }
        if let Health::Busy { attempt } = self.states[idx] {
            self.states[idx] = match attempt {
                0 => Health::Idle,
                _ => Health::Backoff {
                    retry_at: now,
                    attempt,
                },
            };
        }
    }

    fn disconnect(&mut self, tag: u32, now: Instant) {
        let idx = tag as usize;
        if idx >= self.states.len() {
            return;
        }
        if matches!(self.states[idx], Health::Dead) {
            return;
        }
        if self.overrides.get(idx).is_some_and(|o| o.is_some()) {
            self.states[idx] = Health::Dead;
            return;
        }
        let retry_at = self.retry_at(now, 1);
        self.states[idx] = Health::Backoff {
            retry_at,
            attempt: 1,
        };
    }

    fn kill(&mut self, tag: u32) {
        let idx = tag as usize;
        if idx >= self.states.len() {
            return;
        }
        self.states[idx] = Health::Dead;
    }

    fn revive(&mut self) {
        for state in self.states.iter_mut() {
            if matches!(state, Health::Dead | Health::Backoff { .. }) {
                *state = Health::Idle;
            }
        }
    }
}
