use std::mem;

use egress::reserve::{self, Bounds};
use metadata::batch::{Error, Plan};
use o3::{
    base::cell::region,
    buffer::{resident::bound, storage},
};
use reserve::exact;

use crate::link::egress::{
    self, data,
    load::credit,
    metadata,
    reserve::{demand, error},
};

pub(super) mod batch;
pub(in crate::link) mod flight;
pub(super) mod load;
mod prepared;
mod released;

pub(in crate::link) use prepared::{Preparation, Reservation};
pub(in crate::link) use released::Released;
pub(in crate::link) enum Change {
    Applied,
    Stale,
    Invalid(egress::Invalid),
}

pub struct Queue<
    'a,
    'p,
    'd,
    const IOV: usize,
    B = storage::Shared,
    D: demand::Mode = demand::Fallible,
> {
    pub(super) entries: metadata::Queue<'a, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
}

impl<'a, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Queue<'a, 'p, 'd, IOV, B, D>
{
    pub fn plan(&self, bounds: Bounds) -> Result<reserve::Plan<'a, 'p, 'd, B, D>, error::Lane> {
        reserve::Plan::new(self.entries, bounds)
    }

    pub fn offer<S>(
        &self,
        value: egress::NonEmpty<S>,
    ) -> Result<exact::Offer<'a, 'p, 'd, B, D, S>, exact::Unavailable<S>> {
        exact::Offer::new(self.entries, value)
    }

    pub fn reborrow(&self) -> Queue<'a, 'p, 'd, IOV, B, D> {
        Queue {
            entries: self.entries,
        }
    }

    pub fn try_enqueue(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bytes: B,
    ) -> egress::Enqueue<B> {
        self.try_load(token, egress::Load::new(bytes))
    }

    pub fn try_insert(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<B>,
    ) -> Result<(), error::Capacity<B>> {
        metadata::Prepared::prepare(self.entries, token, load)?.retain();
        Ok(())
    }

    pub fn try_load(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::Load<B>,
    ) -> egress::Enqueue<B> {
        let load = match load.try_nonempty() {
            Ok(load) => load,
            Err(rejected) => return egress::Enqueue::Rejected(rejected.into()),
        };
        match self.entries.admit_load(token, load) {
            metadata::Admission::Committed => egress::Enqueue::Committed,
            metadata::Admission::Rejected(rejected) => egress::Enqueue::Rejected(rejected),
        }
    }

    /// Cancels this lane's outstanding credit wait, if one is registered.
    pub fn cancel_wait(&self, token: &mut region::Token<region::Identity<'d>>) {
        credit::Lane::new(self.entries).cancel(token)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.state().is_empty()
    }

    pub fn pending(&self) -> egress::Pending {
        match self.entries.state().is_empty() {
            true => egress::Pending::Absent,
            false => egress::Pending::Present,
        }
    }
}

impl<'a, 'p, 'd, const IOV: usize, B> Queue<'a, 'p, 'd, IOV, B> {
    pub fn try_batch<'token, R, E>(
        &self,
        token: &'token mut region::Token<region::Identity<'d>>,
        plan: Plan,
        build: impl for<'batch> FnOnce(egress::Batch<'batch, 'a, 'token, 'p, 'd, B>) -> Result<R, E>,
    ) -> Result<R, Error<E>> {
        use metadata::batch;

        batch::Draft::try_new(self.entries, token, plan)
            .map_err(batch::Error::Admission)?
            .try_build(|draft| build(egress::Batch::new(draft)))
    }
}

impl<'a, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, G: bound::Bound, const PREFIX: usize>
    Queue<'a, 'p, 'd, IOV, B, demand::Guaranteed<G, PREFIX>>
{
    /// Returns the retained queue-capacity proof, or `None` for direct-only storage.
    pub fn prove(&self) -> Option<reserve::Plan<'a, 'p, 'd, B, demand::Guaranteed<G, PREFIX>>> {
        let bounds = self.entries.pool.bounds()?;
        Some(reserve::Plan::proven(self.entries, bounds))
    }
}

const _: () = {
    assert!(mem::size_of::<Queue<'static, 'static, 'static, 32>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<
            Queue<
                'static,
                'static,
                'static,
                32,
                storage::Shared,
                demand::Guaranteed<bound::AtMost<0>, 1>,
            >,
        >() == 2 * mem::size_of::<usize>()
    );
};
