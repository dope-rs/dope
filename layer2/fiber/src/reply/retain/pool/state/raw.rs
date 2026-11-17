use std::{cell, mem::size_of};

use o3::{mem, mem::rc};

use crate::reply::retain::{grant::payload, pool::state::Refs};

pub(in crate::reply::retain::pool) struct Axis<A: mem::raw::Sum<3, usize>> {
    refs: Refs,
    amount: cell::Cell<Option<A>>,
}

impl<A: mem::raw::Sum<3, usize>> Axis<A> {
    pub(super) const fn new() -> Self {
        Self {
            refs: Refs::new(),
            amount: cell::Cell::new(None),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.refs.is_active()
    }

    pub(super) fn take(&mut self) -> Option<A> {
        self.amount.get_mut().take()
    }

    pub(in crate::reply::retain::pool) fn try_retain(&self) -> Result<(), rc::Error> {
        // SAFETY: Credit cloning proves an existing live edge for this axis.
        unsafe { mem::raw::Edge::try_retain(&self.refs) }
    }

    pub(in crate::reply::retain::pool) fn try_hold(&self) -> Result<(), rc::Error> {
        self.refs.try_hold()
    }

    pub(in crate::reply::retain::pool) fn release(&self) -> Option<A> {
        if self.refs.release() != rc::Release::Last {
            return None;
        }
        self.amount.take()
    }

    /// # Safety
    /// The enclosing transfer owns this disjoint amount from the entry's issuing State.
    pub(in crate::reply::retain::pool) unsafe fn activate(&self, amount: A) {
        let amount = match self.amount.take() {
            // SAFETY: both disjoint amounts belong to the same admitted total.
            Some(stored) => unsafe { stored.add_raw(amount) },
            None => amount,
        };
        self.amount.set(Some(amount));
    }
}

const _: () = {
    assert!(size_of::<Axis<payload::Values>>() == 2 * size_of::<usize>());
    assert!(size_of::<Axis<payload::Bytes>>() == 2 * size_of::<usize>());
};
