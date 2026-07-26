use std::cell::Cell;
use std::mem::take;
use std::time::Instant;

use o3::collections::CellSlab;

use super::{Action, DialKey, Dialer};
use dope_core::driver::token::SlotIndex;
use dope_core::io::socket::addr::Addr;
use dope_net::Transport;

enum ExplicitTag {}

#[derive(Default)]
enum DialSlot<A, O> {
    #[default]
    Empty,
    Queued(A, O),
    Dialing(A, O),
}

enum PendingTransition {
    Connect,
    Retry,
    Skip,
}

struct ExplicitSlot<A, O> {
    state: Cell<DialSlot<A, O>>,
    binding: Cell<Option<SlotIndex>>,
    pending_next: Cell<Option<DialKey>>,
    pending_prev: Cell<Option<DialKey>>,
}

impl<A, O> ExplicitSlot<A, O> {
    fn queued(addr: A, config: O) -> Self {
        Self {
            state: Cell::new(DialSlot::Queued(addr, config)),
            binding: Cell::new(None),
            pending_next: Cell::new(None),
            pending_prev: Cell::new(None),
        }
    }
}

struct ExplicitState<'a, A, O> {
    slots: &'a CellSlab<ExplicitSlot<A, O>, ExplicitTag>,
    key: DialKey,
    state: DialSlot<A, O>,
}

impl<'a, A, O> ExplicitState<'a, A, O> {
    fn take(slots: &'a CellSlab<ExplicitSlot<A, O>, ExplicitTag>, key: DialKey) -> Option<Self> {
        let state = slots.update_parts(key.parts(), |slot| slot.state.take())?;
        Some(Self { slots, key, state })
    }

    fn get(&self) -> &DialSlot<A, O> {
        &self.state
    }
}

impl<A, O> Drop for ExplicitState<'_, A, O> {
    fn drop(&mut self) {
        let state = Cell::new(take(&mut self.state));
        let _ = self.slots.update_parts(self.key.parts(), |slot| {
            let current = slot.state.take();
            if matches!(current, DialSlot::Empty) {
                slot.state.set(state.take());
            } else {
                slot.state.set(current);
            }
        });
    }
}

pub struct Explicit<T: Transport> {
    slots: CellSlab<ExplicitSlot<T::Addr, T::StreamConfig>, ExplicitTag>,
    pending: Cell<Option<DialKey>>,
    pending_tail: Cell<Option<DialKey>>,
}

pub struct ExplicitDialer<'a, T: Transport> {
    source: &'a Explicit<T>,
}

impl<T: Transport> Copy for ExplicitDialer<'_, T> {}

impl<T: Transport> Clone for ExplicitDialer<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Transport> Default for Explicit<T> {
    fn default() -> Self {
        Self {
            slots: CellSlab::with_capacity(0),
            pending: Cell::new(None),
            pending_tail: Cell::new(None),
        }
    }
}

impl<T: Transport> Explicit<T> {
    #[doc(hidden)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: CellSlab::with_capacity(capacity),
            pending: Cell::new(None),
            pending_tail: Cell::new(None),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub fn resize(&mut self, max_connections: usize) {
        if max_connections > self.capacity() {
            self.slots.grow_to(max_connections);
        }
    }

    pub fn dialer(&self) -> ExplicitDialer<'_, T> {
        ExplicitDialer { source: self }
    }

    #[doc(hidden)]
    pub fn dial_shared(&self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        let key = self.slots.insert(ExplicitSlot::queued(addr, config)).ok()?;
        let key = DialKey::from_slab(key);
        self.push_pending(key);
        Some(key)
    }

    fn update_slot<R>(
        &self,
        key: DialKey,
        f: impl FnOnce(&mut ExplicitSlot<T::Addr, T::StreamConfig>) -> R,
    ) -> Option<R> {
        self.slots.update_parts(key.parts(), f)
    }

    fn push_pending(&self, key: DialKey) {
        let tail = self.pending_tail.replace(Some(key));
        unsafe {
            self.update_slot(key, |slot| {
                slot.pending_prev.set(tail);
                slot.pending_next.set(None);
            })
            .unwrap_unchecked()
        };
        match tail {
            Some(tail) => unsafe {
                self.update_slot(tail, |slot| slot.pending_next.set(Some(key)))
                    .unwrap_unchecked()
            },
            None => self.pending.set(Some(key)),
        }
    }

    fn pop_pending(&self) -> Option<DialKey> {
        let key = self.pending.take()?;
        let next = unsafe {
            self.update_slot(key, |slot| {
                slot.pending_prev.set(None);
                slot.pending_next.take()
            })
            .unwrap_unchecked()
        };
        self.pending.set(next);
        match next {
            Some(next) => unsafe {
                self.update_slot(next, |slot| slot.pending_prev.set(None))
                    .unwrap_unchecked()
            },
            None => self.pending_tail.set(None),
        }
        Some(key)
    }

    fn remove_pending(&self, key: DialKey) {
        let Some((prev, next)) = self.update_slot(key, |slot| {
            (slot.pending_prev.take(), slot.pending_next.take())
        }) else {
            return;
        };
        if prev.is_none()
            && next.is_none()
            && self.pending.get() != Some(key)
            && self.pending_tail.get() != Some(key)
        {
            return;
        }
        match prev {
            Some(prev) => unsafe {
                self.update_slot(prev, |slot| slot.pending_next.set(next))
                    .unwrap_unchecked()
            },
            None => self.pending.set(next),
        }
        match next {
            Some(next) => unsafe {
                self.update_slot(next, |slot| slot.pending_prev.set(prev))
                    .unwrap_unchecked()
            },
            None => self.pending_tail.set(prev),
        }
    }

    fn free(&self, key: DialKey) -> Option<SlotIndex> {
        self.remove_pending(key);
        let slot = self.slots.remove_parts(key.parts())?;
        let binding = slot.binding.get();
        drop(slot);
        binding
    }

    #[doc(hidden)]
    pub fn kill_shared(&self, key: DialKey) -> Option<SlotIndex> {
        self.free(key)
    }

    fn poll_connect_shared(&self) -> Action {
        while let Some(key) = self.pop_pending() {
            let transition = self.update_slot(key, |slot| match slot.state.take() {
                DialSlot::Queued(addr, config) => {
                    slot.state.set(DialSlot::Dialing(addr, config));
                    PendingTransition::Connect
                }
                DialSlot::Empty => {
                    slot.state.set(DialSlot::Empty);
                    PendingTransition::Retry
                }
                DialSlot::Dialing(addr, config) => {
                    slot.state.set(DialSlot::Dialing(addr, config));
                    PendingTransition::Skip
                }
            });
            match transition {
                Some(PendingTransition::Connect) => return Action::Connect { key },
                Some(PendingTransition::Retry) => {
                    self.push_pending(key);
                    return Action::Idle;
                }
                Some(PendingTransition::Skip) | None => {}
            }
        }
        Action::Idle
    }

    fn sock_addr_shared(&self, key: DialKey) -> Option<Addr> {
        let state = ExplicitState::take(&self.slots, key)?;
        match state.get() {
            DialSlot::Queued(addr, _) | DialSlot::Dialing(addr, _) => T::to_sock_addr(addr).ok(),
            DialSlot::Empty => None,
        }
    }

    fn socket_params_shared(&self, key: DialKey) -> Option<(i32, i32, i32)> {
        let state = ExplicitState::take(&self.slots, key)?;
        match state.get() {
            DialSlot::Queued(addr, _) | DialSlot::Dialing(addr, _) => Some(T::socket_params(addr)),
            DialSlot::Empty => None,
        }
    }

    fn stream_config_shared(&self, key: DialKey) -> Option<T::StreamConfig> {
        let state = ExplicitState::take(&self.slots, key)?;
        match state.get() {
            DialSlot::Queued(_, config) | DialSlot::Dialing(_, config) => Some(*config),
            DialSlot::Empty => None,
        }
    }

    fn connect_outcome_shared(&self, key: DialKey, success: bool) {
        if !success {
            self.free(key);
        }
    }

    fn connect_deferred_shared(&self, key: DialKey) {
        let queued = self.update_slot(key, |slot| match slot.state.take() {
            DialSlot::Dialing(addr, config) => {
                slot.state.set(DialSlot::Queued(addr, config));
                true
            }
            state => {
                slot.state.set(state);
                false
            }
        });
        if queued == Some(true) {
            self.push_pending(key);
        }
    }

    fn set_binding(&self, key: DialKey, binding: SlotIndex) {
        let _ = self.update_slot(key, |slot| slot.binding.set(Some(binding)));
    }
}

impl<T: Transport> Dialer<T> for ExplicitDialer<'_, T> {
    fn resize(&mut self, max_connections: usize) {
        assert!(self.source.capacity() >= max_connections);
    }

    fn dial(&mut self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        self.source.dial_shared(addr, config)
    }

    fn poll_connect(&mut self, _now: Instant) -> Action {
        self.source.poll_connect_shared()
    }

    fn has_pending(&self) -> bool {
        self.source.pending.get().is_some()
    }

    fn sock_addr(&self, key: DialKey) -> Option<Addr> {
        self.source.sock_addr_shared(key)
    }

    fn socket_params(&self, key: DialKey) -> Option<(i32, i32, i32)> {
        self.source.socket_params_shared(key)
    }

    fn stream_config(&self, key: DialKey) -> Option<T::StreamConfig> {
        self.source.stream_config_shared(key)
    }

    fn connect_outcome(&mut self, key: DialKey, success: bool, _now: Instant) {
        self.source.connect_outcome_shared(key, success);
    }

    fn connect_deferred(&mut self, key: DialKey, _now: Instant) {
        self.source.connect_deferred_shared(key);
    }

    fn disconnect(&mut self, key: DialKey, _now: Instant) {
        self.source.free(key);
    }

    fn kill(&mut self, key: DialKey) {
        self.source.free(key);
    }

    fn bind(&mut self, key: DialKey, binding: SlotIndex) {
        self.source.set_binding(key, binding);
    }
}
