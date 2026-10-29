use std::{mem, task};

use dope_core::driver::schedule::ready;
use o3::{base::cell::region, buffer::resident::bound};

use crate::link::egress::{
    self,
    load::credit,
    metadata,
    reserve::{Bounds, Vacant, demand, error},
};

/// Queue-bound geometry proof reusable across capacity polls.
#[must_use]
pub struct Plan<'queue, 'p, 'd, B, D: demand::Mode = demand::Fallible> {
    queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
    bounds: Bounds,
}

impl<'p, B, D: demand::Mode> Clone for Plan<'_, 'p, '_, B, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'p, B, D: demand::Mode> Copy for Plan<'_, 'p, '_, B, D> {}

impl<'queue, 'p, 'd, B, D: demand::Mode> Plan<'queue, 'p, 'd, B, D> {
    pub(in crate::link::egress) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        bounds: Bounds,
    ) -> Result<Self, error::Lane> {
        let amount = bounds.amount();
        if !queue.fits(amount) {
            return Err(error::Lane::new(bounds));
        }
        Ok(Self::proven(queue, bounds))
    }

    pub(in crate::link::egress) const fn proven(
        queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        bounds: Bounds,
    ) -> Self {
        Self { queue, bounds }
    }

    pub const fn bounds(self) -> Bounds {
        self.bounds
    }

    pub fn poll_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<Vacant<'queue, 'token, 'p, 'd, B, D>, egress::Admission>> {
        let Some(shape) = self.bounds.shape() else {
            return task::Poll::Ready(Ok(Vacant::empty(token, self.bounds)));
        };
        self.queue
            .poll_acquire(token, credit::Queued::from_bounds(self.bounds), target)
            .map(|permit| {
                let permit = permit.map_err(|cause| egress::Admission::Tickets {
                    lane: self.queue.lane,
                    cause,
                })?;
                Vacant::try_new(self.bounds, shape, token, permit)
            })
    }

    pub fn try_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Result<Vacant<'queue, 'token, 'p, 'd, B, D>, egress::Admission> {
        let Some(shape) = self.bounds.shape() else {
            return Ok(Vacant::empty(token, self.bounds));
        };
        let permit = self.queue.try_acquire(token, self.bounds.amount())?;
        Vacant::try_new(self.bounds, shape, token, permit)
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Plan<'static, 'static, 'static, ()>>() == 6 * word);
    assert!(
        mem::size_of::<Plan<'static, 'static, 'static, (), demand::Guaranteed<bound::AtMost<0>, 1>>>(
        ) == 6 * word
    );
    assert!(
        mem::size_of::<
            Option<Plan<'static, 'static, 'static, (), demand::Guaranteed<bound::AtMost<0>, 1>>>,
        >() == 6 * word
    );
};
