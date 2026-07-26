use std::cell::Cell;
use std::task::Poll;

use crate::raw::task::Waker;
use dope::driver::token::Token;
use dope::manifold::connector::source::DialKey;

#[derive(Default)]
enum Slot<'d> {
    #[default]
    Vacant,
    Pending(DialKey),
    Waiting(DialKey, Waker<'d>),
    Settled(DialKey, Outcome),
}

pub(crate) enum Outcome {
    Connected(Token),
    Failed,
}

pub(crate) struct Pending<'d> {
    slots: Box<[Cell<Slot<'d>>]>,
}

impl<'d> Pending<'d> {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            slots: (0..cap).map(|_| Cell::new(Slot::Vacant)).collect(),
        }
    }

    pub(crate) fn reserve(&self, key: DialKey) {
        self.slots[key.index() as usize].set(Slot::Pending(key));
    }

    pub(crate) fn settle(&self, key: DialKey, value: Outcome) {
        let Some(slot) = self.slots.get(key.index() as usize) else {
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

    pub(crate) fn poll(&self, key: DialKey, waker: Waker<'d>) -> Poll<Outcome> {
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return Poll::Pending;
        };
        match slot.take() {
            Slot::Settled(current, value) if current == key => Poll::Ready(value),
            Slot::Pending(current) | Slot::Waiting(current, _) if current == key => {
                slot.set(Slot::Waiting(key, waker));
                Poll::Pending
            }
            state => {
                slot.set(state);
                Poll::Pending
            }
        }
    }

    pub(crate) fn cancel(&self, key: DialKey) {
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return;
        };
        match slot.take() {
            Slot::Pending(current) | Slot::Waiting(current, _) | Slot::Settled(current, _)
                if current == key => {}
            state => {
                slot.set(state);
            }
        }
    }
}
