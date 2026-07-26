use std::cell::Cell;

use crate::raw::task::Waker;
use dope::manifold::connector::source::DialKey;

pub(crate) trait Key: Copy + Eq {
    fn index(self) -> usize;
}

impl Key for DialKey {
    fn index(self) -> usize {
        self.index() as usize
    }
}

#[derive(Default)]
enum Slot<'d, K, R> {
    #[default]
    Vacant,
    Pending(K),
    Waiting(K, Waker<'d>),
    Settled(K, R),
}

pub(crate) struct Pending<'d, K, R> {
    slots: Box<[Cell<Slot<'d, K, R>>]>,
}

pub(crate) enum Resolve<R> {
    Ready(R),
    Pending,
}

impl<'d, K: Key, R> Pending<'d, K, R> {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            slots: (0..cap).map(|_| Cell::new(Slot::Vacant)).collect(),
        }
    }

    pub(crate) fn reserve(&self, key: K) {
        self.slots[key.index()].set(Slot::Pending(key));
    }

    pub(crate) fn settle(&self, key: K, value: R) {
        let Some(slot) = self.slots.get(key.index()) else {
            return;
        };
        match slot.take() {
            Slot::Pending(current) if current == key => slot.set(Slot::Settled(key, value)),
            Slot::Waiting(current, waiter) if current == key => {
                slot.set(Slot::Settled(key, value));
                waiter.wake();
            }
            state => slot.set(state),
        }
    }

    pub(crate) fn poll(&self, key: K, waker: Waker<'d>) -> Resolve<R> {
        let Some(slot) = self.slots.get(key.index()) else {
            return Resolve::Pending;
        };
        match slot.take() {
            Slot::Settled(current, value) if current == key => Resolve::Ready(value),
            Slot::Pending(current) | Slot::Waiting(current, _) if current == key => {
                slot.set(Slot::Waiting(key, waker));
                Resolve::Pending
            }
            state => {
                slot.set(state);
                Resolve::Pending
            }
        }
    }

    pub(crate) fn cancel(&self, key: K) -> bool {
        let Some(slot) = self.slots.get(key.index()) else {
            return false;
        };
        match slot.take() {
            Slot::Pending(current) | Slot::Waiting(current, _) if current == key => false,
            Slot::Settled(current, _) if current == key => true,
            state => {
                slot.set(state);
                false
            }
        }
    }
}
