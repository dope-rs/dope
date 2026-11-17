use std::{marker, mem, task};

use dope_core::driver::schedule::ready;
use o3::{base::cell::region, buffer::resident::bound};

use crate::link::egress::{
    self, data,
    load::credit,
    metadata,
    reserve::{Bounds, Geometry, Vacant, demand, error, geometry},
};

/// Queue-bound geometry proof reusable across capacity polls.
#[must_use]
pub struct Plan<
    'queue,
    'p,
    'd,
    B: data::raw::Payload<'d>,
    D: demand::Mode = demand::Fallible,
    G: Geometry = Bounds,
> {
    queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D, B::Position>,
    bounds: G,
}

/// One acquisition into the reservation retaining this exact token loan.
#[must_use]
pub struct Acquire<'queue, 'token, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry> {
    queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D, B::Position>,
    scope: marker::PhantomData<(&'token mut region::Token<region::Identity<'d>>, G)>,
}

impl<'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry> Clone
    for Plan<'_, 'p, 'd, B, D, G>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry> Copy
    for Plan<'_, 'p, 'd, B, D, G>
{
}

impl<'queue, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry>
    Plan<'queue, 'p, 'd, B, D, G>
{
    pub(in crate::link::egress) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D, B::Position>,
        bounds: G,
    ) -> Result<Self, error::Lane> {
        if !queue.fits(geometry::queued(bounds, bounds.shape()).amount()) {
            return Err(error::Lane::new(bounds.bounds()));
        }
        Ok(Self::proven(queue, bounds))
    }

    pub(in crate::link::egress) const fn proven(
        queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D, B::Position>,
        bounds: G,
    ) -> Self {
        Self { queue, bounds }
    }

    pub fn prepare<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> (
        Vacant<'queue, 'token, 'p, 'd, B, D, G>,
        Acquire<'queue, 'token, 'p, 'd, B, D, G>,
    ) {
        (
            Vacant::empty(token, self.bounds),
            Acquire {
                queue: self.queue,
                scope: marker::PhantomData,
            },
        )
    }

    pub fn poll_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<Vacant<'queue, 'token, 'p, 'd, B, D, G>, egress::Admission>> {
        let (mut vacant, acquire) = self.prepare(token);
        acquire
            .poll_reserve(&mut vacant, target)
            .map(|result| result.map(|()| vacant))
    }

    pub fn try_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Result<Vacant<'queue, 'token, 'p, 'd, B, D, G>, egress::Admission> {
        let (mut vacant, acquire) = self.prepare(token);
        let Some(shape) = vacant.bounds.shape() else {
            return Ok(vacant);
        };
        let (token, _) = vacant.state.parts_mut();
        let credit = acquire.queue.try_acquire(
            token,
            geometry::queued(vacant.bounds, Some(shape)).amount(),
        )?;
        vacant.acquire(shape, credit)?;
        Ok(vacant)
    }
}

impl<'queue, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode>
    Plan<'queue, 'p, 'd, B, D>
{
    pub const fn bounds(self) -> Bounds {
        self.bounds
    }
}

impl<'queue, 'token, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry>
    Acquire<'queue, 'token, 'p, 'd, B, D, G>
{
    pub fn poll_reserve(
        self,
        vacant: &mut Vacant<'queue, 'token, 'p, 'd, B, D, G>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<(), egress::Admission>> {
        let Some(shape) = vacant.bounds.shape() else {
            return task::Poll::Ready(Ok(()));
        };
        let (token, _) = vacant.state.parts_mut();
        let credit = match self.queue.poll_acquire(
            token,
            geometry::queued(vacant.bounds, Some(shape)),
            target,
        ) {
            task::Poll::Pending => return task::Poll::Pending,
            task::Poll::Ready(Err(cause)) => {
                return task::Poll::Ready(Err(egress::Admission::Tickets {
                    lane: self.queue.lane,
                    cause,
                }));
            }
            task::Poll::Ready(Ok(credit)) => credit,
        };
        // The affine token loan forbids another reservation in this domain while
        // this acquisition exists. The paired vacant is still in its empty phase.
        task::Poll::Ready(vacant.acquire(shape, credit))
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Plan<'static, 'static, 'static, &'static [u8]>>() == 6 * word);
    assert!(
        mem::size_of::<
            Plan<'static, 'static, 'static, &'static [u8], demand::Guaranteed<bound::AtMost<0>, 1>>,
        >() == 6 * word
    );
    assert!(
        mem::size_of::<
            Option<
                Plan<
                    'static,
                    'static,
                    'static,
                    &'static [u8],
                    demand::Guaranteed<bound::AtMost<0>, 1>,
                >,
            >,
        >() == 6 * word
    );
};
