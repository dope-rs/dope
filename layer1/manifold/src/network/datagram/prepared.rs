use std::net;

use dope_core::{
    driver::{
        self,
        retained::flight,
        route::{
            self, Identity,
            kind::{Recv, Send},
        },
        storage::Resident,
    },
    io::datagram::Lease,
};
use o3::buffer::{self, pool, resident::Budget};

use crate::{
    app::creation,
    network::datagram::{
        self, hook, packet, send,
        socket::{binding, priority, receive, sender},
    },
};

/// Driver-bound resources awaiting their one socket binding.
#[must_use = "prepared datagram resources must be consumed by one endpoint"]
pub struct Prepared<
    'd,
    ID: Identity,
    B: buffer::Residency + buffer::raw::Source = pool::Cursor,
    M: send::Mode = send::Plain,
> {
    pub(in crate::network::datagram) sender: sender::Sender<'d, ID, B, M, ()>,
    pub(in crate::network::datagram) flights: flight::Need<
        (
            <ID as route::Materialize<Recv>>::With,
            <ID as route::Materialize<Send>>::With,
        ),
        2,
    >,
    pub(in crate::network::datagram) retention: packet::Retainer<'d>,
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    Prepared<'d, ID, B, M>
{
    pub fn bind<H>(
        self,
        addr: net::SocketAddr,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<datagram::Endpoint<'d, ID, H, B, M>, creation::Error>
    where
        H: datagram::Ingress<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    {
        let (binding, target, floor) = binding::Binding::bind(addr, driver)?;
        let socket = self.finish(binding, target, floor, driver)?;
        Ok(datagram::Endpoint { socket, handler })
    }

    pub fn adopt<'g: 'd, H>(
        self,
        lease: Lease<'g>,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<datagram::Endpoint<'d, ID, H, B, M>, creation::Error>
    where
        H: datagram::Ingress<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    {
        let (binding, target, floor) = binding::Binding::adopt(lease, driver)?;
        let socket = self.finish(binding, target, floor, driver)?;
        Ok(datagram::Endpoint { socket, handler })
    }

    fn finish(
        self,
        binding: binding::Binding<'d, ID>,
        target: route::Target<'d, <ID as route::Materialize<Recv>>::With>,
        floor: Option<route::Epoch>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<datagram::Socket<'d, ID, B, M>, creation::Error> {
        let flights = driver
            .storage
            .reborrow()
            .try_flights(self.flights)
            .map_err(creation::Error::Allocation)?;
        let (recv, send) = flights.slots();
        let receive = receive::Receive::new(target, binding.descriptor(), self.retention);
        Ok(datagram::Socket {
            receive,
            recv_flights: recv,
            sender: self.sender.commit(send, floor),
            binding,
            priority: priority::Priority::Application,
        })
    }

    pub(in crate::network::datagram) fn budget(
        bytes: usize,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Option<Budget<'d>>, creation::Error> {
        if bytes == 0 {
            return Ok(None);
        }
        Ok(Some(
            driver
                .storage
                .budget(Resident::bytes(bytes))
                .map_err(creation::Error::Resident)?,
        ))
    }
}
