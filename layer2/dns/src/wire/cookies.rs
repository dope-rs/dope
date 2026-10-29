use std::mem;

use o3::buffer::{bytes, storage::inline};

use crate::{
    lookup::error,
    wire::{Offset, packet},
};

pub(crate) const CLIENT_BYTES: usize = 8;
pub(crate) const MIN_SERVER_BYTES: usize = 8;
pub(crate) const MAX_SERVER_BYTES: usize = 32;
pub(in crate::wire) const OPTION: u16 = 10;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Client(u64);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Server(inline::Bytes<MAX_SERVER_BYTES>);

impl Client {
    pub(crate) const fn from_bytes(value: [u8; CLIENT_BYTES]) -> Self {
        Self(u64::from_be_bytes(value))
    }

    pub(crate) const fn into_bytes(self) -> [u8; CLIENT_BYTES] {
        self.0.to_be_bytes()
    }
}

impl Server {
    pub(in crate::wire) fn from_options(
        options: &[u8],
        expected: Client,
    ) -> Result<Option<Self>, error::Cookie> {
        let packet = packet::Packet::try_new(options).map_err(error::Cookie::Packet)?;
        packet.scope(|packet| {
            let mut cursor = packet.cursor(Offset::ZERO);
            let mut cookie = None;
            while cursor.position().get() < options.len() {
                let offset = cursor.position();
                let code = cursor.u16().map_err(error::Cookie::Packet)?;
                let length = cursor.u16().map_err(error::Cookie::Packet)?;
                let data = cursor.bytes(length).map_err(error::Cookie::Packet)?;
                if code != OPTION {
                    continue;
                }
                if cookie.is_some() {
                    return Err(error::Cookie::Duplicate {
                        offset: offset.raw(),
                    });
                }
                let [a, b, c, d, e, f, g, h, server @ ..] = data else {
                    return Err(error::Cookie::ClientLength {
                        actual: data.len(),
                        required: CLIENT_BYTES,
                    });
                };
                let client = u64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]);
                if client != expected.0 {
                    return Err(error::Cookie::Client {
                        actual: client,
                        expected: expected.0,
                    });
                }
                if server.len() < MIN_SERVER_BYTES {
                    return Err(error::Cookie::ServerLength {
                        actual: server.len(),
                        minimum: MIN_SERVER_BYTES,
                        maximum: MAX_SERVER_BYTES,
                    });
                }
                let prefix = bytes::Prefix::try_new(server).map_err(error::Cookie::Capacity)?;
                cookie = Some(Self(inline::Bytes::from(prefix)));
            }
            Ok(cookie)
        })
    }
}

impl<'a> From<&'a Server> for bytes::Prefix<'a, MAX_SERVER_BYTES> {
    fn from(server: &'a Server) -> Self {
        Self::from(&server.0)
    }
}

const _: () = assert!(mem::size_of::<Client>() == CLIENT_BYTES);
const _: () = assert!(mem::size_of::<Server>() == MAX_SERVER_BYTES + 1);
