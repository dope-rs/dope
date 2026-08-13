use dope_core::driver::{flight, route, schedule};
use dope_net::{
    link::{egress, pool, slot::types},
    wire,
};
use o3::{
    cell::region,
    collections::{self, fixed::pinned::recycle, slab},
};

use crate::listener::{self, connection, writer::resources};

type Pool<'d, const ID: u8, T, W, C, M> = pool::Pool<
    'd,
    ID,
    T,
    W,
    connection::State<'d, ID, C>,
    M,
    resources::Payload<'d, ID>,
    { listener::IOV_CAP },
>;

pub(in crate::listener) struct Prepared<'d, const ID: u8> {
    writes: resources::Arena<'d, ID>,
}

pub(in crate::listener) struct Owner<'d, const ID: u8, T, W, C, M>
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    pool: Pool<'d, ID, T, W, C, M>,
    writes: resources::Arena<'d, ID>,
}

pub(in crate::listener) struct Egress<'a, 'd, const ID: u8, W: wire::Wire, C> {
    pub(in crate::listener) flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
    pub(in crate::listener) connection:
        &'a mut types::Connection<'d, ID, W, connection::State<'d, ID, C>>,
    pub(in crate::listener) queue:
        egress::Queue<'a, 'd, { listener::IOV_CAP }, resources::Payload<'d, ID>>,
    retention: resources::Retention<'a, 'd, ID>,
}

impl<'d, const ID: u8> Prepared<'d, ID> {
    pub(in crate::listener) fn try_new(
        _domain: &region::Token<'d>,
        config: egress::Config,
        direct_flights: u32,
    ) -> Result<Self, collections::AllocationError> {
        let header_slots = config
            .entry_capacity()
            .min(config.resident_capacity() / resources::WRITE_BUF_CAP as u32);
        Ok(Self {
            writes: resources::Arena {
                direct: recycle::Pool::try_with_capacity(
                    slab::Capacity::new(direct_flights),
                    |_| resources::Flight::new(),
                )?,
                headers: recycle::Pool::try_with_capacity(
                    slab::Capacity::new(header_slots),
                    |_| resources::HeaderStorage::new(),
                )?,
            },
        })
    }
}

impl<'d, const ID: u8, T, W, C, M> Owner<'d, ID, T, W, C, M>
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    pub(in crate::listener) fn new(
        pool: Pool<'d, ID, T, W, C, M>,
        prepared: Prepared<'d, ID>,
    ) -> Self {
        Self {
            pool,
            writes: prepared.writes,
        }
    }

    pub(in crate::listener) const fn pool(&self) -> &Pool<'d, ID, T, W, C, M> {
        &self.pool
    }

    pub(in crate::listener) const fn pool_mut(&mut self) -> &mut Pool<'d, ID, T, W, C, M> {
        &mut self.pool
    }

    pub(in crate::listener) fn egress_mut(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> Option<Egress<'_, 'd, ID, W, C>> {
        let Self { pool, writes } = self;
        let pool::EgressMut {
            flights,
            connection,
            queue,
        } = pool.egress_mut(key)?;
        Some(Egress {
            flights,
            connection,
            queue,
            retention: resources::Retention::new(writes),
        })
    }
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C> Egress<'a, 'd, ID, W, C> {
    pub(in crate::listener) fn context<'step>(
        &'step mut self,
        work: schedule::Application<'step, 'd>,
    ) -> connection::Ctx<'step, 'd, ID, W, C>
    where
        'd: 'step,
    {
        connection::Ctx::new(
            self.connection,
            self.flights,
            self.retention.reborrow(),
            self.queue.reborrow(),
            work,
        )
    }
}

impl<const ID: u8, T, W, C, M> Drop for Owner<'_, ID, T, W, C, M>
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    fn drop(&mut self) {
        assert!(
            self.pool.inspection().is_empty(),
            "listener write owner dropped before its retained connections quiesced"
        );
    }
}
