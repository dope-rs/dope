use crate::runtime::brand;

use o3::buffer::{self, resident::Shared};

use crate::conn;
use crate::conn::stream::access::Usage;
use crate::conn::stream::{
    Access, Error, id, resident,
    state::{receive::recv::map, transmit::queue},
};
use crate::stream;
use crate::wire::stream::{Direction, Id};
use std::mem;

/// Scoped immutable stream state.
#[repr(transparent)]
pub struct View<'access, 'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> {
    connection: &'access conn::Machine<DOMAIN, B, D>,
    brand: brand::Token<'conn>,
}

impl<'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment>
    View<'_, 'conn, DOMAIN, B, D>
{
    pub(in crate::conn) fn new<'access>(
        conn: &'access conn::Machine<DOMAIN, B, D>,
        brand: brand::Token<'conn>,
    ) -> View<'access, 'conn, DOMAIN, B, D> {
        View {
            connection: conn,
            brand,
        }
    }

    fn permits(&self, id: Id, access: Access) -> bool {
        if matches!(id.direction(), Direction::Bidi) {
            return true;
        }
        match access {
            Access::Send => id.initiator() == self.connection.peer.local,
            Access::Receive => id.initiator() != self.connection.peer.local,
        }
    }

    pub fn resolve_send(&self, id: Id) -> Result<id::Send<'conn>, Error> {
        if !self.permits(id, Access::Send) {
            return Err(Error::InvalidDirection);
        }
        Ok(id::Send::new(self.brand, id))
    }

    pub fn resolve_recv(&self, id: Id) -> Result<id::Receive<'conn>, Error> {
        if !self.permits(id, Access::Receive) {
            return Err(Error::InvalidDirection);
        }
        Ok(id::Receive::new(self.brand, id))
    }

    pub fn resolve_bidi(&self, id: Id) -> Result<id::Bidi<'conn>, Error> {
        if matches!(id.direction(), Direction::Uni) {
            return Err(Error::InvalidDirection);
        }
        Ok(id::Bidi::new(self.brand, id))
    }

    pub fn is_recv_eof(&self, id: id::Receive<'conn>) -> bool {
        let id = id.id();
        let streams = &self.connection.streams;
        streams
            .receive
            .map
            .find(map::Id::new(id), &streams.resident)
            .is_some_and(|resolved| resolved.state().is_eof())
            || streams.is_recv_done(id)
    }

    pub(super) fn try_recv_eof(
        &self,
        handle: resident::Handle<'conn>,
    ) -> Result<bool, resident::Stale> {
        let local = handle.local();
        let streams = &self.connection.streams;
        Ok(streams
            .receive
            .map
            .try_find(local, &streams.resident)?
            .is_some_and(|resolved| resolved.state().is_eof())
            || streams.is_recv_done(local.id()))
    }

    pub fn has_recv_fin(&self, id: id::Receive<'conn>) -> bool {
        let id = id.id();
        let streams = &self.connection.streams;
        streams
            .receive
            .map
            .find(map::Id::new(id), &streams.resident)
            .and_then(|resolved| resolved.state().final_size())
            .is_some()
            || streams.is_recv_done(id)
    }

    /// Whether this stream's send half still accepts new bytes.
    /// Excludes finished, reset, and retired halves, independently of
    /// connection readiness, flow credit, and buffer capacity.
    pub fn is_send_open(&self, id: id::Send<'conn>) -> bool {
        let id = id.id();
        let streams = &self.connection.streams;
        if !streams.can_access(id, Access::Send) {
            return false;
        }
        streams
            .transmit
            .map()
            .find(id, &streams.resident)
            .map_or_else(
                || id.initiator() != self.connection.peer.local && !streams.is_send_done(id),
                |entry| matches!(entry.phase(), queue::Phase::Open),
            )
    }

    pub fn stopped(&self, id: id::Send<'conn>) -> Option<crate::errors::Code> {
        self.connection
            .streams
            .transmit
            .map()
            .find(id.id(), &self.connection.streams.resident)
            .and_then(|entry| entry.stop_error())
    }

    pub fn has_events(&self) -> bool {
        !self.connection.streams.events.is_empty()
    }

    pub fn buffered(&self, id: id::Send<'conn>) -> usize {
        let streams = &self.connection.streams;
        streams
            .transmit
            .map()
            .find(id.id(), &streams.resident)
            .map_or(0, |entry| entry.buffered_len())
    }

    pub fn usage(&self) -> Usage<'_, D> {
        Usage::new(&self.connection.streams.transmit)
    }
}

const _: () = {
    assert!(
        mem::size_of::<View<'_, '_, 0, Vec<u8>, Shared<'_>>>()
            == mem::size_of::<&conn::Machine<0, Vec<u8>, Shared<'_>>>()
    );
};
