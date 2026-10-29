use std::{array, mem::size_of};

use o3::{
    mem,
    mem::{Rejected, credit::fair},
};

use crate::reply::{
    retain,
    retain::grant::payload::{Credit, Grant, Tag},
    usage,
};

pub(in crate::reply) struct Sum<'credits>(Credit<'credits, [usize; 3]>);

impl<'credits> Sum<'credits> {
    pub(in crate::reply) fn new(lane: retain::raw::Lane<'credits, 3, Tag>) -> Self {
        Self(mem::raw::Grant::empty(lane))
    }

    pub(in crate::reply) fn try_acquire<A: mem::raw::Amount<3, usize>>(
        &mut self,
        amount: A,
    ) -> Result<(), fair::Cause<usize, 3>> {
        let addition = self.0.try_extend(amount.get()).map_err(Rejected::discard)?;
        addition.merge();
        Ok(())
    }

    pub(in crate::reply) fn drain(&mut self) -> Self {
        Self(self.0.drain())
    }

    pub(in crate::reply) fn len(&self) -> usize {
        self.0.amount()[0]
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        let amount = self.0.amount();
        usage::Usage::new(usage::Bytes::new(amount[2]), usage::Values::new(amount[1]))
    }

    /// # Safety
    /// The grant must be disjoint and originate in this sum's exact lane.
    pub(in crate::reply) unsafe fn merge<A>(&mut self, grant: Grant<'credits, A>)
    where
        A: mem::raw::Amount<3, usize>,
    {
        let grant = grant.0.into_array();
        // SAFETY: the caller's owning operation retains the exact same-source
        // acquisition; Bound excludes target replacement until commit.
        unsafe { self.0.bind_mut(grant) }.merge();
    }

    /// # Safety
    /// The amount must belong to one still-accounted original member of this sum.
    pub(in crate::reply) unsafe fn take<A>(&mut self, amount: A) -> Grant<'credits, A>
    where
        A: mem::raw::Amount<3, usize>,
    {
        // SAFETY: the original item is a member of this exclusively borrowed sum.
        Grant(unsafe { self.0.bind_mut(Parts(amount)) }.split(()))
    }
}

struct Parts<A>(A);

// SAFETY: bound removal owns an exact source member and its exclusive sum.
unsafe impl<A: mem::raw::Amount<3, usize>> mem::raw::Exact<3, usize, [usize; 3]> for Parts<A> {
    type Input = ();
    type Left = [usize; 3];
    type Right = A;
    fn parts(self, source: [usize; 3], (): ()) -> (Self::Left, A) {
        let part = self.0.get();
        (
            array::from_fn(|axis| source[axis].wrapping_sub(part[axis])),
            self.0,
        )
    }
}

const _: () = assert!(size_of::<Sum<'_>>() == 4 * size_of::<usize>());
