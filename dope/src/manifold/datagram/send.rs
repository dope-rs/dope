use std::net::SocketAddr;
use std::num::NonZeroU16;

use o3::buffer::Lease;

use crate::manifold::datagram::Packet;
use dope_core::backend::Gso;
use dope_core::io::socket::addr::InetAddr;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

use crate::DriverContext;
use std::slice::from_ref;

pub(super) enum Payload<'d> {
    Owned(Vec<u8>),
    Buffer(Lease<'d>),
    Packet(Packet<'d>),
}

impl Payload<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Owned(payload) => payload.len(),
            Self::Buffer(payload) => payload.len(),
            Self::Packet(packet) => packet.as_ref().len(),
        }
    }
}

pub(super) struct Outgoing<'d> {
    addr: SocketAddr,
    payload: Payload<'d>,
    segment_size: u16,
}

impl<'d> Outgoing<'d> {
    pub(super) fn plain(payload: Payload<'d>, addr: SocketAddr) -> Self {
        Self {
            addr,
            payload,
            segment_size: 0,
        }
    }

    pub(super) fn gso(payload: Vec<u8>, segment_size: NonZeroU16, addr: SocketAddr) -> Self {
        Self {
            addr,
            payload: Payload::Owned(payload),
            segment_size: segment_size.get(),
        }
    }
}

pub(super) struct SendOp<'d> {
    payload: Payload<'d>,
    target: SocketAddr,
    addr: InetAddr,
    iov: IoVec,
    msg: MsgHdr,
    segment_size: u16,
    cmsg: Gso,
}

impl<'d> SendOp<'d> {
    pub(super) fn new(out: Outgoing<'d>) -> Self {
        let target = out.addr;
        Self {
            payload: out.payload,
            target,
            addr: InetAddr::from_std(target),
            iov: IoVec::empty(),
            msg: MsgHdr::empty(),
            segment_size: out.segment_size,
            cmsg: Gso::new(),
        }
    }

    pub(super) fn fill_msghdr(&mut self) -> &MsgHdr {
        let payload = match &self.payload {
            Payload::Owned(payload) => payload.as_slice(),
            Payload::Buffer(payload) => payload.as_ref(),
            Payload::Packet(packet) => packet.as_ref(),
        };
        self.iov = IoVec::from_slice(payload);
        let name_ptr = self.addr.mut_ptr();
        let name_len = self.addr.socklen();
        self.msg.set_name_ptr(name_ptr.cast(), name_len);
        self.msg.set_iov(from_ref(&self.iov));
        self.cmsg.attach(&mut self.msg, self.segment_size);
        &self.msg
    }

    pub(super) fn into_outgoing(self) -> Outgoing<'d> {
        Outgoing {
            addr: self.target,
            payload: self.payload,
            segment_size: self.segment_size,
        }
    }

    pub(super) fn finish(self, driver: &mut DriverContext<'_, 'd>) -> Option<usize> {
        match self.payload {
            Payload::Packet(packet) => {
                let len = packet.as_ref().len();
                packet.release(driver);
                Some(len)
            }
            Payload::Buffer(payload) => Some(payload.len()),
            payload => Some(payload.len()),
        }
    }
}
