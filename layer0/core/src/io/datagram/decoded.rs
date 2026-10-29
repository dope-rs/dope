use std::{mem, net};

use o3::buffer::bytes::scope;

use crate::io::{datagram, recv, socket};

pub enum Decoded<'d> {
    Packet {
        source: net::SocketAddr,
        payload: recv::Lease<'d>,
    },
    Address {
        error: socket::Error,
        buffer: recv::Lease<'d>,
    },
    Rejected(datagram::Failure<'d>),
    Truncated(datagram::Failure<'d>),
}

impl<'d> Decoded<'d> {
    pub fn decode(buffer: recv::Lease<'d>) -> Self {
        scope::Owner::with(
            buffer.into_inner(),
            |owner| match datagram::Projection::parse(owner.view()) {
                datagram::Projection::Packet { source, payload } => Self::Packet {
                    source,
                    payload: recv::Lease::new(owner.project(payload)),
                },
                datagram::Projection::Address(error) => Self::Address {
                    error,
                    buffer: recv::Lease::new(owner.into_inner()),
                },
                datagram::Projection::Rejected(cause) => Self::Rejected(datagram::Failure::new(
                    recv::Lease::new(owner.into_inner()),
                    cause,
                )),
                datagram::Projection::Truncated(cause) => Self::Truncated(datagram::Failure::new(
                    recv::Lease::new(owner.into_inner()),
                    cause,
                )),
            },
        )
    }
}

const _: () = assert!(
    mem::size_of::<Decoded<'static>>() == mem::size_of::<(net::SocketAddr, recv::Lease<'static>)>()
);
const _: () = assert!(mem::size_of::<Decoded<'static>>() <= 7 * mem::size_of::<usize>());
