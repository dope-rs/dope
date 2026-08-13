use std::{io, net, pin, time};

use dope::{
    core::driver::{self, schedule},
    manifold::{
        datagram::{self, packet},
        service::attach,
    },
};
use o3::{buffer, cell::region};

use crate::{storage::queries, wire::query};

pub(crate) struct Plan {
    buffers: buffer::Pool,
    config: datagram::Config,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
pub struct Datagram<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> {
    #[pin]
    #[forward]
    inner: datagram::Endpoint<'d, ID, Handler<'d, LANES, ADDRESSES>>,
}

impl Plan {
    pub(crate) fn new(outbound: usize) -> io::Result<Self> {
        let retained_send_bytes =
            outbound
                .checked_mul(query::MAX_QUERY_BYTES)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "DNS outbound byte bound overflows the platform",
                    )
                })?;
        let config = datagram::Config::new(outbound, retained_send_bytes, outbound)?;
        let layout = buffer::Layout::new(outbound, query::MAX_QUERY_BYTES)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let buffers = buffer::Pool::try_from_layout(layout)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        Ok(Self { buffers, config })
    }

    pub(crate) fn bind<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize>(
        self,
        storage: attach::Bound<'d, crate::Storage<LANES, ADDRESSES>>,
        address: net::SocketAddr,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Datagram<'d, ID, LANES, ADDRESSES>> {
        let handler = Handler {
            storage,
            buffers: self.buffers,
        };
        let inner = datagram::Endpoint::bind_with_config(address, handler, self.config, driver)?;
        Ok(Datagram { inner })
    }
}

#[doc(hidden)]
pub struct Handler<'d, const LANES: usize, const ADDRESSES: usize> {
    storage: attach::Bound<'d, crate::Storage<LANES, ADDRESSES>>,
    buffers: buffer::Pool,
}

impl<'d, const LANES: usize, const ADDRESSES: usize> Handler<'d, LANES, ADDRESSES> {
    fn send<const ID: u8>(
        &mut self,
        mut socket: pin::Pin<&mut datagram::Socket<'d, ID>>,
        now: time::Instant,
        work: schedule::Application<'_, 'd>,
    ) {
        let queries = &self.storage.queries;
        let buffers = &self.buffers;
        loop {
            let Some(buffer) = buffers.try_acquire_buffer() else {
                return;
            };
            let (mut buffer, outgoing) = match queries.admit_datagram(now, work, buffer) {
                schedule::Admission::Item((buffer, queries::Start::Prepared(outgoing))) => {
                    (buffer, outgoing)
                }
                schedule::Admission::Item((_, queries::Start::Processed))
                | schedule::Admission::Empty
                | schedule::Admission::Exhausted => return,
            };
            let encoded = outgoing.with_query(|query| query.encode_datagram(&mut buffer));
            if !matches!(encoded, Some(Ok(()))) {
                return;
            }
            if socket
                .as_mut()
                .queue_buffer(buffer, outgoing.server())
                .is_err()
            {
                return;
            }
            outgoing.commit();
        }
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> datagram::Handler<'d, ID>
    for Handler<'d, LANES, ADDRESSES>
{
    fn packet<'turn>(
        &mut self,
        source: net::SocketAddr,
        packet: packet::Packet<'turn, 'd>,
        _socket: pin::Pin<&'turn mut datagram::Socket<'d, ID>>,
        now: time::Instant,
    ) {
        self.storage
            .queries
            .accept_datagram(source, packet.as_ref(), now);
    }

    fn pre_park(
        &mut self,
        socket: pin::Pin<&mut datagram::Socket<'d, ID>>,
        now: time::Instant,
        work: schedule::Application<'_, 'd>,
    ) {
        self.send(socket, now, work);
    }

    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let _ = region;
        if self.storage.queries.needs_datagram() && self.buffers.available() != 0 {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
}
