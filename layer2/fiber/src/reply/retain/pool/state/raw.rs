use std::{cell, mem::size_of};

use o3::{mem, mem::rc};

use crate::reply::retain::{grant::payload, pool::state::Refs};

pub(in crate::reply::retain::pool) struct Axis<'credits, A: mem::raw::Sum<3, usize>> {
    refs: Refs,
    grant: cell::Cell<Option<payload::Grant<'credits, A>>>,
}

/// Final Pool ownership excludes every usable key; abandoned edges end with this epoch.
impl<A: mem::raw::Sum<3, usize>> Drop for Axis<'_, A> {
    fn drop(&mut self) {
        if let Some(grant) = self.grant.get_mut().take() {
            grant.into_inner().release();
        }
    }
}

impl<'credits, A: mem::raw::Sum<3, usize>> Axis<'credits, A> {
    pub(super) const fn new() -> Self {
        Self {
            refs: Refs::new(),
            grant: cell::Cell::new(None),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.refs.is_active()
    }

    pub(in crate::reply::retain::pool) fn try_retain(&self) -> Result<(), rc::Error> {
        // SAFETY: Credit cloning proves an existing live edge for this axis.
        unsafe { mem::raw::Edge::try_retain(&self.refs) }
    }

    pub(in crate::reply::retain::pool) fn try_hold(&self) -> Result<(), rc::Error> {
        self.refs.try_hold()
    }

    pub(in crate::reply::retain::pool) fn release(&self) -> Option<payload::Grant<'credits, A>> {
        if self.refs.release() != rc::Release::Last {
            return None;
        }
        self.grant.take()
    }

    /// # Safety
    /// The enclosing transfer must retain the same entry and issuing lane as
    /// every existing grant in this axis, and own the incoming disjoint grant.
    pub(in crate::reply::retain::pool) unsafe fn activate(
        &self,
        mut grant: payload::Grant<'credits, A>,
    ) {
        if let Some(stored) = self.grant.take() {
            let mut stored = stored.into_inner();
            // SAFETY: this axis stores only grants returned through one exact
            // pool lane, whose admitted total bounds the component sum.
            unsafe { stored.bind_mut(grant.into_inner()) }.merge();
            grant = payload::Grant::new(stored);
        }
        self.grant.set(Some(grant));
    }
}

const _: () = {
    assert!(
        size_of::<Option<payload::Grant<'_, payload::Values>>>()
            == size_of::<payload::Grant<'_, payload::Values>>()
    );
    assert!(
        size_of::<Option<payload::Grant<'_, payload::Bytes>>>()
            == size_of::<payload::Grant<'_, payload::Bytes>>()
    );
    assert!(size_of::<Axis<'_, payload::Values>>() == 3 * size_of::<usize>());
    assert!(size_of::<Axis<'_, payload::Bytes>>() == 3 * size_of::<usize>());
};
