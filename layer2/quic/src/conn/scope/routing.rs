use crate::conn::machine::Reset;
use crate::proto::path::Peer;
use o3::buffer;
use std::time;

use crate::{
    conn, stream,
    wire::{packet, stream::Initiator},
};
use conn::{output::transmit, wire::path};
use packet::version;
use path::{local::routes, reset};

#[repr(transparent)]
pub(crate) struct Routing<'access, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> {
    machine: &'access mut conn::Machine<DOMAIN, B, D>,
}

impl<const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> Routing<'_, DOMAIN, B, D> {
    pub(super) fn new(machine: &mut conn::Machine<DOMAIN, B, D>) -> Routing<'_, DOMAIN, B, D> {
        Routing { machine }
    }

    pub(crate) fn bind_peer(&mut self, address: Peer) {
        self.machine.path.bind(address);
    }

    pub(crate) fn is_new(&self, address: Peer) -> bool {
        !self.machine.is_closing() && self.machine.path.ingress().is_new(address)
    }

    pub(crate) fn begin(
        &mut self,
        address: Peer,
        bytes: usize,
        outer_bytes: usize,
        token: Option<[[u8; 8]; 2]>,
    ) {
        self.machine.path.begin(address, bytes, outer_bytes, token);
    }

    pub(crate) fn end_datagram(&mut self) {
        self.machine.path.end(&self.machine.control);
    }

    pub(crate) fn peer(&self) -> Option<Peer> {
        self.machine.path.ingress().peer()
    }

    pub(crate) fn try_validate(
        &mut self,
        now: time::Instant,
        token: Option<[u8; 8]>,
    ) -> Result<(), transmit::Error> {
        if self.machine.is_closing() {
            return Ok(());
        }
        let (_, pto) = self.machine.try_pto().map_err(transmit::Error::Timing)?;
        let effects = self
            .machine
            .path
            .try_service(now, pto, token, &mut self.machine.control)
            .map_err(transmit::Error::Path)?;
        self.machine.peer.reconcile.merge(effects);
        Ok(())
    }

    pub(crate) fn is_rejected(&mut self, vn: &version::Negotiation<'_>) -> bool {
        if !matches!(self.machine.peer.local, Initiator::Client)
            || self.machine.egress.lifecycle.state != conn::State::Handshaking
            || self.machine.path.origin.peer_first_scid.is_some()
            || self.machine.control.probe().is_retry()
            || vn.dcid().as_ref() != self.machine.path.local.cid().as_ref()
            || vn.scid().as_ref() != self.machine.path.origin.original_dcid.as_slice()
        {
            return false;
        }
        if !vn.contains(packet::QUIC_V1) {
            self.machine.close_now();
            return true;
        }
        false
    }

    pub(crate) fn reset(&mut self, source: Peer, token: reset::Token) -> Reset {
        self.machine.reset(Some(source), token)
    }

    pub(crate) fn sync_tokens(&mut self) {
        self.machine.path.peer.sync_tokens();
    }

    pub(crate) fn drain(&mut self) -> Option<routes::Drain> {
        self.machine.path.local.drain()
    }
}
