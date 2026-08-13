use std::{cell, io, mem, task};

use dope::{
    core::driver::schedule::ready::completion,
    manifold::connector::{attempt, connection},
};
use o3::collections;

#[derive(Default)]
enum Slot<'d, const ID: u8> {
    #[default]
    Vacant,
    Pending(attempt::Id<'d, ID>, Option<completion::Waker<'d>>),
    Settled(attempt::Id<'d, ID>, Outcome<'d, ID>),
}

pub(crate) struct Stale;

pub(crate) enum Outcome<'d, const ID: u8> {
    Connected(connection::Id<'d, ID>),
    Failed(io::Error),
}

const _: () = assert!(mem::size_of::<Outcome<'static, 0>>() <= mem::size_of::<[usize; 2]>());

pub(super) struct Pending<'d, const ID: u8> {
    slots: Box<[cell::Cell<Slot<'d, ID>>]>,
}

impl<'d, const ID: u8> Pending<'d, ID> {
    pub(crate) fn try_with_capacity(cap: usize) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            slots: collections::BoxSliceExt::try_box_with(cap, |_| cell::Cell::new(Slot::Vacant))?,
        })
    }

    pub(crate) fn reserve(&self, key: attempt::Id<'d, ID>) -> bool {
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return false;
        };
        match slot.take() {
            Slot::Vacant => {
                slot.set(Slot::Pending(key, None));
                true
            }
            state => {
                slot.set(state);
                false
            }
        }
    }

    pub(crate) fn settle(
        &self,
        key: attempt::Id<'d, ID>,
        value: Outcome<'d, ID>,
    ) -> Result<(), Outcome<'d, ID>> {
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return Err(value);
        };
        match slot.take() {
            Slot::Pending(current, waiter) if current == key => {
                slot.set(Slot::Settled(key, value));
                if let Some(waiter) = waiter {
                    waiter.wake();
                }
                Ok(())
            }
            state => {
                slot.set(state);
                Err(value)
            }
        }
    }

    pub(crate) fn poll(
        &self,
        key: attempt::Id<'d, ID>,
        wake: completion::Waker<'d>,
    ) -> Result<task::Poll<Outcome<'d, ID>>, Stale> {
        use std::task::Poll;
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return Err(Stale);
        };
        match slot.take() {
            Slot::Settled(current, value) if current == key => Ok(Poll::Ready(value)),
            Slot::Pending(current, _) if current == key => {
                slot.set(Slot::Pending(key, Some(wake)));
                Ok(Poll::Pending)
            }
            state => {
                slot.set(state);
                Err(Stale)
            }
        }
    }

    pub(crate) fn cancel(&self, key: attempt::Id<'d, ID>) -> bool {
        let Some(slot) = self.slots.get(key.index() as usize) else {
            return false;
        };
        match slot.take() {
            Slot::Pending(current, _) | Slot::Settled(current, _) if current == key => true,
            state => {
                slot.set(state);
                false
            }
        }
    }
}
