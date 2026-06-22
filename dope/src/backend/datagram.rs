use std::net::SocketAddr;

use crate::backend::lend::{BidGuard, Lend};

pub enum Outcome<'a> {
    Packet { src: SocketAddr, payload: &'a [u8] },
    Truncated { src: SocketAddr, partial: &'a [u8] },
    Empty,
    Error(i32),
}

pub trait Datagram: Lend {
    fn recv_packet<'a>(
        &'a mut self,
        len: u32,
        bid: u16,
        msghdr: &libc::msghdr,
    ) -> (Outcome<'a>, BidGuard<'a, Self>);
}
