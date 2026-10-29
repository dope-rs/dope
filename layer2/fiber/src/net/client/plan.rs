use std::marker;

use dope::{
    core::driver::route::{Identity, KeyTag},
    net::{
        self,
        wire::{self, domain},
    },
};

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
    manifold::{app::creation, client::attempt::queue::mailbox},
    net::{
        client,
        port::{self, table::Table},
        read,
    },
};

pub struct Plan<T: net::Transport, W: wire::Spec, ID: Identity = KeyTag<0>> {
    layout: port::Plan,
    domain: domain::Owner<W, ID>,
    _kind: marker::PhantomData<fn() -> T>,
}

impl<T: net::Transport, W: wire::Spec, ID: Identity> Plan<T, W, ID> {
    pub fn new(capacity: Capacity, storage: W::Storage<ID>) -> Self {
        Self::finish(port::Plan::new(capacity), domain::Owner::new(storage))
    }

    pub fn try_new(capacity: Capacity) -> Result<Self, creation::Error>
    where
        W::Bootstrap: domain::Allocate<W>,
    {
        let domain = domain::Owner::try_new(capacity)?;
        Ok(Self::finish(port::Plan::new(capacity), domain))
    }

    pub fn wire_domain(&self) -> &domain::Owner<W, ID> {
        &self.domain
    }

    pub fn with_receive(mut self, receive: read::Config) -> Result<Self, read::Capacity> {
        self.layout = self.layout.with_receive(receive)?;
        Ok(self)
    }

    pub fn try_build<'w, 'd: 'w>(
        &self,
        mut ctx: storage::Context<'_, 'd>,
    ) -> Result<client::Port<'w, 'd, T, W, ID>, creation::Error> {
        let slots = self.layout.count();
        let conns = Table::try_new(self.layout, false, ctx.region())
            .map_err(creation::Error::Allocation)?;
        let pending = mailbox::Domain::try_new(slots, &mut ctx)?;
        Ok(client::Port::from_parts(conns, pending))
    }

    fn finish(layout: port::Plan, domain: domain::Owner<W, ID>) -> Self {
        Self {
            layout,
            domain,
            _kind: marker::PhantomData,
        }
    }
}

impl<T: net::Transport, W: wire::Spec, ID: Identity> settings::Resource for Plan<T, W, ID> {
    fn include_in(&self, needs: Needs) -> Result<Needs, errors::Claim> {
        let outbound = demands::Outbound::scale::<1>(self.layout.connections());
        needs.include(
            Claim::new(settings::Routes::singleton::<ID>())
                .files(demands::Files::only_out(outbound)),
        )
    }
}
