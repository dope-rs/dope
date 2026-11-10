use std::mem;

use o3::buffer::{self, resident};

use crate::conn;
use crate::stream;

/// The first admitted local or peer close, retained until the connection is dropped.
#[derive(Debug)]
pub enum Close {
    LocalApplication {
        error_code: crate::errors::Code,
        reason: Vec<u8>,
    },
    LocalTransport(crate::wire::varint::VarInt),
    PeerApplication {
        error_code: crate::errors::Code,
        reason: Result<Vec<u8>, o3::alloc::Error>,
    },
    PeerTransport {
        error_code: crate::wire::varint::VarInt,
        frame_type: crate::wire::varint::VarInt,
        reason: Result<Vec<u8>, o3::alloc::Error>,
    },
}

#[repr(transparent)]
pub struct State<
    'conn,
    const DOMAIN: u8,
    B: stream::Recv = Vec<u8>,
    D: buffer::raw::Segment = resident::Shared<'conn>,
> {
    conn: &'conn conn::Machine<DOMAIN, B, D>,
}

impl<'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> State<'conn, DOMAIN, B, D> {
    pub(crate) fn new(conn: &'conn conn::Machine<DOMAIN, B, D>) -> Self {
        Self { conn }
    }

    pub fn state(&self) -> conn::State {
        self.conn.egress.lifecycle.state
    }

    pub fn is_opening(&self) -> bool {
        self.state() == conn::State::Handshaking
    }

    pub fn is_ready(&self) -> bool {
        self.state() == conn::State::Established
    }

    pub fn is_closed(&self) -> bool {
        self.state() == conn::State::Closed
    }

    pub fn is_closing(&self) -> bool {
        self.conn.is_closing()
    }

    pub fn is_confirmed(&self) -> bool {
        self.conn.egress.lifecycle.handshake_confirmed
    }

    pub fn close(&self) -> Option<&'conn Close> {
        self.conn.egress.lifecycle.pending_close.as_ref()
    }

    /// Certificates or raw public keys after authenticated TLS completion.
    pub fn peer_certificates(&self) -> Option<&'conn [rustls::pki_types::CertificateDer<'static>]> {
        self.conn.handshake.peer_certificates()
    }

    /// Whether TLS has accepted early data.
    pub fn is_early(&self) -> bool {
        self.conn.handshake.peer.is_early()
    }
}

const _: () = {
    assert!(mem::size_of::<State<'static, 0>>() == mem::size_of::<&conn::Machine<0, Vec<u8>>>());
};
