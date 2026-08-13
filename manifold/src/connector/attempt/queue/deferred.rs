use std::cell;

use crate::connector::attempt;

struct Stored<'d, T: dope_net::Transport, const ID: u8> {
    key: attempt::Id<'d, ID>,
    plan: attempt::Plan<T>,
}

#[repr(transparent)]
pub(super) struct Deferred<'d, T: dope_net::Transport, const ID: u8> {
    slot: cell::Cell<Option<Stored<'d, T, ID>>>,
}

impl<'d, T: dope_net::Transport, const ID: u8> Deferred<'d, T, ID> {
    pub(super) fn new() -> Self {
        Self {
            slot: cell::Cell::new(None),
        }
    }

    pub(super) fn take_or_create(
        &self,
        key: attempt::Id<'d, ID>,
        target: &attempt::StreamTarget<T::Addr>,
    ) -> attempt::Plan<T> {
        let Some(deferred) = self.slot.take() else {
            return attempt::Plan::new(&target.peer, target.options);
        };
        if deferred.key == key {
            deferred.plan
        } else {
            self.slot.set(Some(deferred));
            attempt::Plan::new(&target.peer, target.options)
        }
    }

    pub(super) fn clear(&self, key: attempt::Id<'d, ID>) {
        let Some(deferred) = self.slot.take() else {
            return;
        };
        if deferred.key != key {
            self.slot.set(Some(deferred));
        }
    }

    pub(super) fn store(&self, key: attempt::Id<'d, ID>, plan: attempt::Plan<T>) -> bool {
        match self.slot.take() {
            None => {
                self.slot.set(Some(Stored { key, plan }));
                true
            }
            Some(deferred) => {
                self.slot.set(Some(deferred));
                false
            }
        }
    }
}
