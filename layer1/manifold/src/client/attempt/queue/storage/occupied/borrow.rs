use std::cell::Cell;

use dope_core::driver::route::Identity;
use o3::depot::{array, queue::tagged};

use crate::client::attempt::{self, queue::storage::Occupied};

/// Payload access without authority to remove a generation held by a lease.
pub(in crate::client::attempt::queue) struct Borrow<E>(E);

impl<E> Borrow<E> {
    pub(in crate::client::attempt::queue::storage) fn new(entry: E) -> Self {
        Self(entry)
    }
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> Borrow<Occupied<'a, 'd, T, ID>> {
    pub(in crate::client::attempt::queue) fn defer(
        mut self,
        plan: attempt::Plan<T>,
        deferred: &Cell<Option<attempt::Id<'d, ID>>>,
    ) {
        if self.0.state().defer(plan) != attempt::Transition::Applied {
            return;
        }
        let first = deferred.get().is_none();
        if first {
            deferred.set(Some(self.0.key));
        }
        let slot = tagged::Vacant::detach(self.0).select(array::Index::at::<0>());
        if first {
            slot.push::<true>();
        } else {
            slot.push::<false>();
        }
    }

    pub(in crate::client::attempt::queue) fn free(mut self) {
        self.0.state().done();
        tagged::Vacant::detach(self.0);
    }
}
