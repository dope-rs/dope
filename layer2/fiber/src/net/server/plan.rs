use std::cell;

use dope::{
    core::driver::route::{Identity, KeyTag},
    net::wire::{self, domain},
};
use o3::{alloc, depot::queue::indexed::shared};

use crate::{
    core::driver::{
        route::table::conn::Capacity,
        settings::{
            self,
            claim::{Claim, Needs},
            demands, errors,
        },
        storage,
    },
    manifold::app::creation,
    net::{
        port::{self, table::Table},
        read, server,
    },
    wait,
};

pub struct Plan<W: wire::Spec, ID: Identity = KeyTag<0>> {
    layout: port::Plan,
    domain: domain::Owner<W, ID>,
}

impl<W: wire::Spec, ID: Identity> settings::Resource for Plan<W, ID> {
    fn include_in(&self, needs: Needs) -> Result<Needs, errors::Claim> {
        let accept = demands::Accept::from(self.layout.connections());
        needs.include(Claim::new(settings::Routes::EMPTY).files(demands::Files::accept(accept)))
    }
}

impl<W: wire::Spec, ID: Identity> Plan<W, ID> {
    pub fn new(cap: Capacity, storage: W::Storage<ID>) -> Self {
        let layout = port::Plan::new(cap);
        Self {
            layout,
            domain: domain::Owner::new(storage),
        }
    }

    pub fn try_new(cap: Capacity) -> Result<Self, creation::Error>
    where
        W::Bootstrap: domain::Allocate<W>,
    {
        let layout = port::Plan::new(cap);
        let domain = domain::Owner::<W, ID>::try_new(layout.connections())?;
        Ok(Self { layout, domain })
    }

    pub fn wire_domain(&self) -> &domain::Owner<W, ID> {
        &self.domain
    }

    pub fn with_receive(mut self, receive: read::Config) -> Result<Self, read::Capacity> {
        self.layout = self.layout.with_receive(receive)?;
        Ok(self)
    }

    /// Borrows this wire owner for the port's entire lifetime.
    ///
    /// ```compile_fail
    /// use dope::{core::driver::storage, net::wire::Identity};
    /// use dope_fiber::net::server::{ListenerPort, Plan};
    ///
    /// fn escape<'w, 'd: 'w>(
    ///     plan: Plan<Identity>,
    ///     context: storage::Context<'_, 'd>,
    /// ) -> Result<ListenerPort<'w, 'd, Identity>, o3::alloc::Error> {
    ///     plan.try_build(context)
    /// }
    /// ```
    pub fn try_build<'w, 'd: 'w>(
        &'w self,
        ctx: storage::Context<'_, 'd>,
    ) -> Result<server::ListenerPort<'w, 'd, W, ID>, alloc::Error> {
        let cap = self.layout.connections();
        let waits = wait::Queue::for_conns(ctx.driver(), cap);
        Ok(server::ListenerPort {
            connections: Table::try_new(self.layout, true, ctx.region())?,
            accepts: shared::Fifo::try_new(cap.entries().get())?,
            waiters: waits,
            accept_error: cell::Cell::new(None),
        })
    }
}
