use std::mem;

use dope::net::link::egress::data;
use o3::buffer::{self, bytes, pool, storage::inline};

use crate::{
    query::family,
    storage::queries::tx::Epoch,
    wire::{cookies, records, tx},
};

const LEGACY_DATAGRAM_BYTES: usize = 512;
const HEADER_BYTES: usize = 12;
const QUESTION_TRAILER_BYTES: usize = 4;
const OPT_RECORD_BYTES: usize = 11;
const OPTION_HEADER_BYTES: usize = 4;
const MAX_COOKIE_OPTION_BYTES: usize =
    OPTION_HEADER_BYTES + cookies::CLIENT_BYTES + cookies::MAX_SERVER_BYTES;
const QUERY_OVERHEAD_BYTES: usize =
    HEADER_BYTES + QUESTION_TRAILER_BYTES + OPT_RECORD_BYTES + MAX_COOKIE_OPTION_BYTES;
pub(crate) const MAX_QUERY_BYTES: usize = QUERY_OVERHEAD_BYTES + crate::host::Name::MAX_WIRE_LEN;
const STREAM_PREFIX_BYTES: usize = 2;
pub(crate) const EDNS_PAYLOAD: u16 = 1232;
pub(crate) const MAX_STREAM_FRAME_BYTES: usize =
    STREAM_PREFIX_BYTES + MAX_QUERY_BYTES - MAX_COOKIE_OPTION_BYTES;

type Buffer = pool::Cursor<pool::Width<MAX_QUERY_BYTES>>;

pub(crate) struct Datagram {
    id: tx::Id,
    /// Preserves allocation identity through delayed transport settlement.
    epoch: Epoch,
    buffer: Buffer,
}

pub(crate) struct Builder<'a> {
    id: tx::Id,
    name: &'a crate::host::Name,
    family: family::Version,
}

impl<'a> Builder<'a> {
    pub(crate) const fn new(
        id: tx::Id,
        name: &'a crate::host::Name,
        family: family::Version,
    ) -> Self {
        Self { id, name, family }
    }

    pub(crate) fn udp(
        self,
        mut out: Buffer,
        epoch: Epoch,
        client: cookies::Client,
        server: Option<&cookies::Server>,
    ) -> Datagram {
        const PREFIX: usize = HEADER_BYTES + 1;
        const NAME: usize = crate::host::Name::MAX_WIRE_LEN - 2;
        const TAIL: usize = MAX_QUERY_BYTES - PREFIX - NAME - cookies::MAX_SERVER_BYTES;

        let id = self.id;
        let server = match server {
            Some(server) => bytes::Prefix::from(server),
            None => bytes::Prefix::new(&[]),
        };
        let len = cookies::CLIENT_BYTES as u16 + server.as_slice().len() as u16;
        let mut header = [0; PREFIX];
        header[..2].copy_from_slice(&id.wire().to_be_bytes());
        header[2] = 1;
        header[5] = 1;
        header[11] = 1;
        let mut tail = [0; TAIL];
        tail[1..3].copy_from_slice(&self.family.kind().wire().to_be_bytes());
        tail[3..5].copy_from_slice(&records::Class::IN.wire().to_be_bytes());
        tail[6..8].copy_from_slice(&records::Kind::OPT.wire().to_be_bytes());
        tail[8..10].copy_from_slice(&EDNS_PAYLOAD.to_be_bytes());
        tail[14..16].copy_from_slice(&(OPTION_HEADER_BYTES as u16 + len).to_be_bytes());
        tail[16..18].copy_from_slice(&cookies::OPTION.to_be_bytes());
        tail[18..20].copy_from_slice(&len.to_be_bytes());
        tail[20..].copy_from_slice(&client.into_bytes());
        let fill = pool::Fill::new(&mut out);
        let (fill, header) = fill.write::<PREFIX, { MAX_QUERY_BYTES - PREFIX }>((&header).into());
        let (fill, name) =
            fill.write::<NAME, { TAIL + cookies::MAX_SERVER_BYTES }>(self.name.into());
        Self::name(&mut header[HEADER_BYTES], name);
        let (fill, _) = fill.write::<TAIL, { cookies::MAX_SERVER_BYTES }>((&tail).into());
        fill.finish(server);
        Datagram {
            id,
            epoch,
            buffer: out,
        }
    }

    pub(crate) fn tcp(self) -> data::Inline<MAX_STREAM_FRAME_BYTES> {
        const PREFIX: usize = STREAM_PREFIX_BYTES + HEADER_BYTES + 1;
        const NAME: usize = crate::host::Name::MAX_WIRE_LEN - 2;
        const TAIL: usize = MAX_STREAM_FRAME_BYTES - PREFIX - NAME;

        let len = (QUERY_OVERHEAD_BYTES - MAX_COOKIE_OPTION_BYTES + self.name.wire_len()) as u16;
        let mut header = [0; PREFIX];
        header[..2].copy_from_slice(&len.to_be_bytes());
        header[2..4].copy_from_slice(&self.id.wire().to_be_bytes());
        header[4] = 1;
        header[7] = 1;
        header[13] = 1;
        let mut tail = [0; TAIL];
        tail[1..3].copy_from_slice(&self.family.kind().wire().to_be_bytes());
        tail[3..5].copy_from_slice(&records::Class::IN.wire().to_be_bytes());
        tail[6..8].copy_from_slice(&records::Kind::OPT.wire().to_be_bytes());
        tail[8..10].copy_from_slice(&EDNS_PAYLOAD.to_be_bytes());
        let mut out = data::Inline::new();
        let fill = inline::Fill::new(&mut out.0);
        let (fill, header) = fill.write::<PREFIX, { NAME + TAIL }>((&header).into());
        let (fill, name) = fill.write::<NAME, TAIL>(self.name.into());
        Self::name(&mut header[STREAM_PREFIX_BYTES + HEADER_BYTES], name);
        fill.finish((&tail).into());
        out
    }

    fn name(first: &mut u8, name: &mut [u8]) {
        let mut label = 0;
        for byte in name.iter_mut().rev() {
            if *byte == b'.' {
                *byte = label;
                label = 0;
            } else {
                label += 1;
            }
        }
        *first = label;
    }
}

impl Datagram {
    pub(crate) const fn transaction(&self) -> (tx::Id, Epoch) {
        (self.id, self.epoch)
    }
}

impl AsRef<[u8]> for Datagram {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl buffer::Residency for Datagram {
    fn resident(&self) -> usize {
        buffer::Residency::resident(&self.buffer)
    }
}

const _: () = {
    assert!(MAX_QUERY_BYTES <= LEGACY_DATAGRAM_BYTES);
    assert!(MAX_STREAM_FRAME_BYTES <= u16::MAX as usize);
    assert!(cookies::MAX_SERVER_BYTES <= u16::MAX as usize);
    assert!(
        mem::size_of::<Datagram>()
            == mem::size_of::<pool::Cursor>() + 2 * mem::align_of::<pool::Cursor>()
    );
    assert!(mem::size_of::<data::Inline<MAX_STREAM_FRAME_BYTES>>() == 286);
};

// SAFETY: a datagram owns its cursor, and shared access cannot change its extent.
unsafe impl buffer::raw::Source for Datagram {}
