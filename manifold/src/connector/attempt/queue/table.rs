use std::cell;

use dope_core::io::socket::option;
use dope_net::link::pool;
use o3::collections::{self, slab};

use crate::connector::{
    attempt,
    attempt::queue::{self, deferred},
};

enum Tag {}

#[derive(Default)]
enum DialSlot<'d, T: dope_net::Transport, const ID: u8> {
    #[default]
    Busy,
    Queued(attempt::StreamTarget<T::Addr>),
    Opening(attempt::StreamTarget<T::Addr>),
    Submitted(Option<option::StreamOptions>, pool::Key<'d, ID>),
    Connected(pool::Key<'d, ID>),
    Active(pool::Key<'d, ID>),
    Cancelled(pool::Key<'d, ID>),
    Terminal,
}

struct Entry<'d, T: dope_net::Transport, const ID: u8> {
    state: cell::Cell<DialSlot<'d, T, ID>>,
}

impl<'d, T: dope_net::Transport, const ID: u8> Entry<'d, T, ID> {
    fn new(target: attempt::StreamTarget<T::Addr>) -> Self {
        Self {
            state: cell::Cell::new(DialSlot::Queued(target)),
        }
    }

    fn release(
        self,
        key: attempt::Id<'d, ID>,
        deferred: &deferred::Deferred<'d, T, ID>,
        pending: &queue::Pending<'d, ID>,
    ) -> Option<pool::Key<'d, ID>> {
        match self.state.into_inner() {
            DialSlot::Queued(_) => {
                deferred.clear(key);
                pending.remove_dial(key);
                None
            }
            DialSlot::Opening(_) | DialSlot::Busy | DialSlot::Terminal => None,
            DialSlot::Submitted(_, binding)
            | DialSlot::Connected(binding)
            | DialSlot::Active(binding) => Some(binding),
            DialSlot::Cancelled(binding) => {
                pending.remove_cancellation(key);
                Some(binding)
            }
        }
    }

    fn remove(
        slots: &slab::Cell<Self, Tag>,
        deferred: &deferred::Deferred<'d, T, ID>,
        pending: &queue::Pending<'d, ID>,
        key: attempt::Id<'d, ID>,
    ) -> Option<pool::Key<'d, ID>> {
        let entry = slots.slots().remove_parts(key.parts())?;
        entry.release(key, deferred, pending)
    }

    fn transition<R>(
        &self,
        transition: impl FnOnce(DialSlot<'d, T, ID>) -> (DialSlot<'d, T, ID>, R),
    ) -> R {
        let state = self.state.take();
        let (state, result) = transition(state);
        self.state.set(state);
        result
    }
}

pub(super) struct Table<'d, T: dope_net::Transport, const ID: u8> {
    slots: slab::Cell<Entry<'d, T, ID>, Tag>,
    pending: queue::Pending<'d, ID>,
    deferred: deferred::Deferred<'d, T, ID>,
}

impl<'d, T: dope_net::Transport, const ID: u8> Table<'d, T, ID> {
    pub(super) fn try_with_capacity(
        capacity: slab::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            slots: slab::Cell::try_with_capacity(capacity)?,
            pending: queue::Pending::try_with_capacity(capacity)?,
            deferred: deferred::Deferred::new(),
        })
    }

    pub(super) fn dial(
        &self,
        target: attempt::StreamTarget<T::Addr>,
    ) -> Option<attempt::Id<'d, ID>> {
        use crate::connector::attempt::Id;
        let key = self.slots.insert(Entry::new(target)).ok()?;
        let key = Id::from_slab(key);
        self.pending.push_dial(key);
        Some(key)
    }

    pub(super) fn free(&self, key: attempt::Id<'d, ID>) -> Option<pool::Key<'d, ID>> {
        let owned = self
            .slots
            .slots()
            .update_parts(key.parts(), |slot| {
                slot.transition(|state| match state {
                    DialSlot::Queued(_) => {
                        self.deferred.clear(key);
                        self.pending.remove_dial(key);
                        (DialSlot::Terminal, true)
                    }
                    DialSlot::Opening(_) | DialSlot::Submitted(_, _) | DialSlot::Connected(_) => {
                        (DialSlot::Terminal, true)
                    }
                    DialSlot::Terminal => (DialSlot::Terminal, true),
                    state => (state, false),
                })
            })
            .unwrap_or(false);
        if owned {
            return None;
        }
        Entry::remove(&self.slots, &self.deferred, &self.pending, key)
    }

    pub(super) fn fail_connect(&self, key: attempt::Id<'d, ID>) {
        let owned = self
            .slots
            .slots()
            .update_parts(key.parts(), |slot| {
                slot.transition(|state| match state {
                    DialSlot::Opening(_) | DialSlot::Submitted(_, _) | DialSlot::Terminal => {
                        (DialSlot::Terminal, true)
                    }
                    state => (state, false),
                })
            })
            .unwrap_or(false);
        if owned {
            return;
        }
        let removed = self.slots.slots().remove_parts_with(key.parts(), |slot| {
            matches!(slot.state.get_mut(), DialSlot::Cancelled(_)).then_some(())
        });
        if let Some((entry, ())) = removed {
            entry.release(key, &self.deferred, &self.pending);
        }
    }

    pub(super) fn cancel(&self, key: attempt::Id<'d, ID>) {
        enum Transition {
            Free,
            Queue,
            None,
        }

        let transition = self.slots.slots().update_parts(key.parts(), |slot| {
            slot.transition(|state| match state {
                state @ (DialSlot::Queued(_) | DialSlot::Opening(_) | DialSlot::Terminal) => {
                    (state, Transition::Free)
                }
                DialSlot::Submitted(_, binding) | DialSlot::Connected(binding) => {
                    (DialSlot::Cancelled(binding), Transition::Queue)
                }
                state => (state, Transition::None),
            })
        });
        match transition {
            Some(Transition::Free) => {
                Entry::remove(&self.slots, &self.deferred, &self.pending, key);
            }
            Some(Transition::Queue) => self.pending.push_cancellation(key),
            Some(Transition::None) | None => {}
        }
    }

    pub(super) fn commit_lease(&self, key: attempt::Id<'d, ID>) {
        enum Transition {
            Complete,
            Free,
            Cancel,
            None,
        }

        let transition = self.slots.slots().update_parts(key.parts(), |slot| {
            slot.transition(|state| match state {
                DialSlot::Connected(binding) => (DialSlot::Active(binding), Transition::Complete),
                DialSlot::Terminal => (DialSlot::Terminal, Transition::Free),
                state
                @ (DialSlot::Queued(_) | DialSlot::Opening(_) | DialSlot::Submitted(_, _)) => {
                    (state, Transition::Cancel)
                }
                state => (state, Transition::None),
            })
        });
        match transition {
            Some(Transition::Free) => {
                Entry::remove(&self.slots, &self.deferred, &self.pending, key);
            }
            Some(Transition::Cancel) => self.cancel(key),
            Some(Transition::Complete | Transition::None) | None => {}
        }
    }

    pub(super) fn take_cancel(&self) -> Option<(attempt::Id<'d, ID>, pool::Key<'d, ID>)> {
        let key = self.pending.pop_cancellation()?;
        let binding = Entry::remove(&self.slots, &self.deferred, &self.pending, key)?;
        Some((key, binding))
    }

    pub(super) fn poll_connect(&self) -> attempt::Action<'d, T, ID> {
        use crate::connector::attempt::Action;
        while let Some(key) = self.pending.pop_dial() {
            let (plan, retry) = self
                .slots
                .slots()
                .update_parts(key.parts(), |slot| {
                    slot.transition(|state| match state {
                        DialSlot::Queued(target) => {
                            let plan = self.deferred.take_or_create(key, &target);
                            (DialSlot::Opening(target), (Some(plan), false))
                        }
                        DialSlot::Busy => (DialSlot::Busy, (None, true)),
                        state => (state, (None, false)),
                    })
                })
                .unwrap_or((None, false));
            if let Some(plan) = plan {
                return Action::Connect { key, plan };
            }
            if retry {
                self.pending.push_dial(key);
                return Action::Idle;
            }
        }
        Action::Idle
    }

    pub(super) fn connect_succeeded(&self, key: attempt::Id<'d, ID>) -> attempt::Transition {
        self.slots
            .slots()
            .update_parts(key.parts(), |slot| {
                slot.transition(|state| match state {
                    DialSlot::Submitted(_, binding) => {
                        (DialSlot::Connected(binding), attempt::Transition::Applied)
                    }
                    state => (state, attempt::Transition::Stale),
                })
            })
            .unwrap_or(attempt::Transition::Stale)
    }

    pub(super) fn connect_deferred(&self, key: attempt::Id<'d, ID>, plan: attempt::Plan<T>) {
        let queued = self.slots.slots().update_parts(key.parts(), |slot| {
            slot.transition(|state| match state {
                DialSlot::Opening(target) => (DialSlot::Queued(target), true),
                state => (state, false),
            })
        });
        if queued == Some(true) {
            if self.deferred.store(key, plan) {
                self.pending.push_dial_front(key);
            } else {
                self.pending.push_dial(key);
            }
        }
    }

    pub(super) fn connect_options(
        &self,
        key: attempt::Id<'d, ID>,
    ) -> Option<option::StreamOptions> {
        self.slots.slots().update_parts(key.parts(), |slot| {
            slot.transition(|state| match state {
                DialSlot::Submitted(options, binding) => {
                    (DialSlot::Submitted(None, binding), options)
                }
                state => (state, None),
            })
        })?
    }

    pub(super) fn set_binding(
        &self,
        key: attempt::Id<'d, ID>,
        binding: pool::Key<'d, ID>,
        options: Option<option::StreamOptions>,
    ) {
        let _ = self.slots.slots().update_parts(key.parts(), |slot| {
            slot.transition(|state| match state {
                DialSlot::Opening(_) => (DialSlot::Submitted(options, binding), ()),
                state => (state, ()),
            })
        });
    }

    pub(super) fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.dials_empty()
    }
}
