use dope_core::driver::{life::finish, route::Identity};
pub(super) mod config;

use data::compose;
use dope_core::{driver, driver::schedule::maint, io::socket::msg};
use o3::{base::cell::region, depot::arena::pool::recycle};

use crate::link::{
    egress::{
        self, create, data,
        load::credit,
        metadata::{self, arena, front},
        queue::flight,
        reserve::demand,
    },
    pool::{self, send},
};

/// Pool-owned retained egress backing shared by generation-owned lanes.
pub(in crate::link) struct Storage<
    'p,
    'd,
    ID: Identity,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> {
    lanes:
        arena::Arena<'p, 'd, B, Option<flight::Lease<'p, 'd, IOV>>, credit::Ready<'p, 'd>, ID, D>,
    flights: &'p recycle::Store<flight::Entry<'d, IOV>>,
}

impl<'p, 'd, ID: Identity, B, const IOV: usize, D: demand::Mode> Storage<'p, 'd, ID, B, IOV, D> {
    pub(in crate::link) fn has_work(&self) -> bool {
        self.lanes.pool.credit.has_work()
    }

    pub(in crate::link) fn maintain(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        work: maint::Work<'_, 'd>,
    ) {
        self.lanes.pool.credit.maintain(token, work);
    }

    pub(in crate::link) fn try_with<R, C>(
        setup: demand::Setup<D>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'pool> FnOnce(Storage<'pool, 'd, ID, B, IOV, D>, &'pool mut C) -> R,
    ) -> Result<R, create::Error> {
        const {
            assert!(IOV != 0, "egress IOV must be non-zero");
            assert!(
                IOV <= msg::MAX_IOVECS,
                "egress IOV exceeds the platform limit"
            );
        }
        let (config, len, mode) = setup.into_parts();
        let mut credits =
            arena::Store::try_lanes(config, len).map_err(create::Error::Allocation)?;
        let arena = {
            let driver = context(&mut *owner);
            arena::Arena::try_driver(&driver.regions().0, mode, &mut credits, driver.driver_ref())?
        };
        let flights = recycle::Store::try_new(arena.config().flights(arena.len()))
            .map_err(create::Error::Allocation)?;
        Ok(build(
            Storage {
                lanes: arena,
                flights: &flights,
            },
            owner,
        ))
    }

    pub(in crate::link) fn pending(&self, key: pool::Key<'d, ID>) -> egress::Pending {
        let slot = self.lanes.bind(key);
        if slot.queue().state().is_empty() {
            egress::Pending::Absent
        } else {
            egress::Pending::Present
        }
    }

    pub(in crate::link) fn clear<'turn>(
        &mut self,
        key: pool::Key<'d, ID>,
        work: maint::Work<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> egress::Clear<B> {
        let slot = self.lanes.bind_mut(key);
        let (lane, queue) = slot.into_parts();
        credit::Lane::new(queue).cancel(driver.region_token());
        if lane.is_some() {
            return egress::Clear::Waiting;
        }
        if queue.state().is_empty() {
            return egress::Clear::Empty;
        }
        let Some(permit) = work.permit() else {
            return egress::Clear::Retry;
        };
        let payload = match permit.run(|| front::take(queue, driver.region_token())) {
            Some(value) => value.into_inner(),
            None => return egress::Clear::Empty,
        };
        let left = if queue.state().is_empty() {
            egress::Remaining::Empty
        } else {
            egress::Remaining::Pending
        };
        egress::Clear::Released {
            payload,
            remaining: left,
        }
    }

    /// Clears one lane after exact backend quiescence. No maintenance permit
    /// is needed because no producer or kernel consumer remains live.
    pub(in crate::link) fn finish(
        &mut self,
        key: pool::Key<'d, ID>,
        finish: &mut finish::Finish<'_, 'd>,
        mut free: impl FnMut(B, &mut region::Token<region::Identity<'d, 1>>),
    ) {
        let slot = self.lanes.bind_mut(key);
        let (lane, queue) = slot.into_parts();
        credit::Lane::new(queue).cancel(finish.region_token());
        *lane = None;
        while let Some(value) = front::take(queue, finish.region_token()) {
            free(value.into_inner(), finish.state_token());
        }
    }
}

impl<'p, 'd, ID: Identity, B: data::raw::Payload<'d>, const IOV: usize, D: demand::Mode>
    Storage<'p, 'd, ID, B, IOV, D>
{
    pub(in crate::link) fn bind_queue<'a>(
        &'a mut self,
        key: pool::Key<'d, ID>,
    ) -> egress::Output<'a, 'p, 'd, IOV, B, D> {
        egress::Output::new(self.lanes.bind_mut(key), self.flights)
    }
}

impl<'p, 'd, ID: Identity, L, R: data::raw::Payload<'d>, const IOV: usize, D: demand::Mode>
    Storage<'p, 'd, ID, compose::Either<L, R>, IOV, D>
{
    pub(in crate::link) fn try_right(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        key: pool::Key<'d, ID>,
        load: egress::NonEmpty<R>,
    ) -> Result<(), send::Rejected<R>> {
        let slot = self.lanes.bind(key);
        metadata::Prepared::prepare(slot.queue(), token, load)
            .map_err(send::Rejected::Egress)?
            .right();
        Ok(())
    }
}
