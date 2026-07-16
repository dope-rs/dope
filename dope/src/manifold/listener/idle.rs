use std::pin::Pin;
use std::time::{Duration, Instant};

use super::Listener;
use super::application::Application;
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;
use dope_core::backend::Sqe;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::SlotIndex;
use o3::collections::SlotQueue;

pub(super) struct IdleSet {
    queue: SlotQueue<Instant>,
    window: Duration,
    cap: usize,
}

impl IdleSet {
    pub(super) fn new(cap: usize, window: Duration) -> Self {
        Self {
            queue: SlotQueue::with_capacity(0),
            window,
            cap,
        }
    }

    fn ensure(&mut self, index: usize) -> bool {
        if index >= self.cap {
            return false;
        }
        if self.queue.capacity() <= index {
            let capacity = self
                .queue
                .capacity()
                .saturating_mul(2)
                .max(index + 1)
                .min(self.cap);
            self.queue.grow_to(capacity);
        }
        true
    }

    pub(super) fn arm(&mut self, idx: SlotIndex, now: Instant) {
        let raw = idx.raw();
        let i = raw as usize;
        if !self.ensure(i) {
            return;
        }
        self.queue.remove(i);
        let deadline = now + self.window;
        let Some(entry) = self.queue.vacant_entry(i) else {
            unreachable!()
        };
        entry.push_back(deadline);
    }

    pub(super) fn cancel(&mut self, idx: SlotIndex) {
        self.queue.remove(idx.raw() as usize);
    }

    pub(super) fn pop_expired(&mut self, now: Instant) -> Option<SlotIndex> {
        let (index, &deadline) = self.queue.front_key_value()?;
        if deadline > now {
            return None;
        }
        self.queue.pop_front();
        Some(SlotIndex::new(index as u32))
    }

    pub(super) fn earliest(&self) -> Option<Instant> {
        self.queue.front().copied()
    }
}

pub(super) trait IdlePhase<'d, const ID: u8, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn drain_idle<F>(
        self: Pin<&mut Self>,
        now: Instant,
        driver: &mut DriverContext<'_, 'd>,
        project: F,
    ) where
        F: Fn(Pin<&mut Self>) -> &mut IdleSet;

    fn close_inherent(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);
}

impl<'d, const ID: u8, A, E> IdlePhase<'d, ID, A, E> for Listener<'d, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn drain_idle<F>(
        mut self: Pin<&mut Self>,
        now: Instant,
        driver: &mut DriverContext<'_, 'd>,
        project: F,
    ) where
        F: Fn(Pin<&mut Self>) -> &mut IdleSet,
    {
        while let Some(idx) = project(self.as_mut()).pop_expired(now) {
            Self::close_inherent(self.as_mut(), idx, driver);
        }
    }

    fn close_inherent(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self.project();
        this.idle.cancel(idx);
        if E::Profile::SEND_DEADLINE.is_some() {
            this.idle_send.cancel(idx);
        }
        if E::Profile::ABS_CONN_AGE.is_some() {
            this.idle_abs_age.cancel(idx);
        }
        let (send_inflight, is_armed, is_closing, cancel_kind, ud) = match this.pool.get(idx) {
            Some(s) => (
                s.core.is_send_inflight(),
                s.core.is_armed(),
                s.core.is_closing(),
                s.core.recv_cancel_kind(),
                s.token(),
            ),
            None => return,
        };
        if send_inflight || is_armed {
            if !is_closing {
                if let Some(slot) = this.pool.get_mut(idx) {
                    slot.core.begin_close();
                }
                if is_armed && !send_inflight {
                    let _ = driver.push(Sqe::cancel(ud, cancel_kind));
                }
            }
            return;
        }
        if this
            .pool
            .get_mut(idx)
            .is_some_and(|s| s.seal_graceful(driver, ud))
        {
            return;
        }
        if let Some(slot) = this.pool.get_mut(idx) {
            this.app.as_mut().close(slot, this.aux);
            if let Some(ip) = slot.state.peer_ip.take() {
                this.accept.release_peer_ip(ip);
            }
        }
        this.pool.try_close(idx, driver);
    }
}
