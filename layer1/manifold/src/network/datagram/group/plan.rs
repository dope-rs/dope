use std::{marker, net, num::NonZero};

use dope_core::{
    driver::{
        self,
        retained::flight,
        route::{self, Identity, indices::SlotIndex, lease, table},
        settings::{self, claim, demands},
    },
    io::socket::{self, install::Install},
};
use o3::{alloc::VecExt as _, base::num::bounded, buffer::pool, depot::queue::indexed::fifo};

use crate::{
    app::creation,
    network::datagram::{group, plan, send, socket::receive::Receive},
};

/// Exact startup admission failure for one group of datagram sockets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Capacity(table::Invalid),
    Files(demands::Overflow),
}

/// One route with a fixed set of exclusively owned datagram sockets.
#[must_use = "a datagram group plan must be declared and consumed once"]
pub struct Plan<ID: Identity> {
    config: plan::Config,
    sockets: bounded::NonZeroU32<1, { route::SLOT_MASK as u32 + 1 }>,
    files: demands::Outbound,
    route: marker::PhantomData<fn() -> ID>,
}

impl<ID: Identity> Plan<ID> {
    /// `sockets` includes the listener at slot zero.
    pub fn try_new(config: plan::Config, sockets: NonZero<u32>) -> Result<Self, Error> {
        let sockets = table::Capacity::try_nonzero(sockets).map_err(Error::Capacity)?;
        let files = demands::Outbound::try_from(sockets.into_usize()).map_err(Error::Files)?;
        Ok(Self {
            config,
            sockets,
            files,
            route: marker::PhantomData,
        })
    }

    pub const fn capacity(&self) -> usize {
        self.sockets.into_usize()
    }

    pub const fn claim(&self) -> claim::Claim {
        claim::Claim::new(settings::Routes::singleton::<ID>())
            .files(demands::Files::only_out(self.files))
            .domains(self.config.domains())
            .schedule(demands::Schedule::fixed::<1, 1>())
    }

    /// Binds the listener and distinct ephemeral member sockets before activation.
    pub fn bind<'d, H: group::Handler<'d, ID>>(
        self,
        first: net::SocketAddr,
        mut members: net::SocketAddr,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<group::raw::Endpoint<'d, ID, H>, creation::Error> {
        members.set_port(0);
        let slots = table::Capacity::from(self.sockets);
        let prepared = plan::Plan::<ID, send::Routed>::new(self.config)
            .prepare_for::<pool::Cursor>(flight::Capacity::table(slots), driver)?;
        let flights = driver
            .storage
            .reborrow()
            .try_flights(prepared.flights)
            .map_err(creation::Error::Allocation)?;
        let (flights, send) = flights.slots();
        let ready = fifo::List::try_with(self.sockets.nonzero(), SlotIndex::ZERO)
            .map_err(creation::Error::Allocation)?;
        let mut storage = Vec::try_new(self.capacity()).map_err(creation::Error::Allocation)?;
        let mut owner = lease::Owner::<ID>::try_reserve(driver).map_err(creation::Error::Route)?;
        for slot in slots {
            let addr = if slot == SlotIndex::ZERO {
                first
            } else {
                members
            };
            let config = socket::Config::for_datagram(
                (slot == SlotIndex::ZERO && addr.port() != 0).then_some(socket::Reuse::Port),
            );
            let (fd, addr) = Install::bind_udp(owner.driver().reborrow(), addr, config)
                .map_err(dope_net::BindingError::Bootstrap)
                .map_err(creation::Error::Binding)?;
            let target = fd.driver().targets().bind(slot, route::Epoch::INITIAL);
            let receive = Receive::new(target, &fd, prepared.retention);
            storage.push(group::Member { receive, fd, addr });
        }
        Ok(group::raw::Endpoint {
            members: storage.into_boxed_slice(),
            sender: prepared.sender.commit(send),
            flights,
            owner: owner.commit(),
            handler,
            ready,
            deferred: false,
            send_first: false,
        })
    }
}
