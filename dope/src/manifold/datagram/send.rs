use std::net::SocketAddr;
use std::rc::Rc;

use o3::buffer::Lease;

use crate::manifold::datagram::Packet;
use dope_core::backend::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
use dope_core::io::socket::addr::InetAddr;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

use crate::DriverContext;
use std::slice::from_ref;

pub(super) enum Payload<'d> {
    Owned(Vec<u8>),
    Buffer(Lease<'d>),
    Packet(Packet<'d>),
    Segment {
        batch: Rc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
}

impl Payload<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Owned(payload) => payload.len(),
            Self::Buffer(payload) => payload.len(),
            Self::Packet(packet) => packet.as_ref().len(),
            Self::Segment { len, .. } => *len,
        }
    }
}

pub(super) struct Outgoing<'d> {
    addr: SocketAddr,
    payload: Payload<'d>,
    segment_size: u16,
}

impl<'d> Outgoing<'d> {
    pub(super) fn visit_segments(
        segments: &[u32],
        mut visit: impl FnMut(usize, usize, Option<u16>),
    ) -> Option<usize> {
        let mut index = 0;
        let mut offset = 0;
        while index < segments.len() {
            let segment = usize::try_from(segments[index]).ok()?;
            if segment == 0 || u16::try_from(segment).is_err() {
                return None;
            }
            let mut end = index + 1;
            while end < segments.len()
                && segments[end - 1] == segments[index]
                && segments[end] <= segments[index]
            {
                end += 1;
            }
            let mut chunk = index;
            while chunk < end {
                let mut chunk_end = chunk;
                let mut bytes = 0;
                while chunk_end < end && chunk_end - chunk < MAX_GSO_SEGMENTS {
                    let next = usize::try_from(segments[chunk_end]).ok()?;
                    if next > MAX_GSO_BYTES.saturating_sub(bytes) {
                        break;
                    }
                    bytes += next;
                    chunk_end += 1;
                }
                if chunk_end == chunk {
                    return None;
                }
                let gso = (chunk_end - chunk > 1).then_some(segment as u16);
                visit(offset, bytes, gso);
                offset += bytes;
                chunk = chunk_end;
            }
            index = end;
        }
        Some(offset)
    }

    pub(super) fn plain(payload: Payload<'d>, addr: SocketAddr) -> Self {
        Self {
            addr,
            payload,
            segment_size: 0,
        }
    }

    pub(super) fn range(
        batch: Rc<Vec<u8>>,
        offset: usize,
        len: usize,
        addr: SocketAddr,
        segment_size: Option<u16>,
    ) -> Self {
        Self {
            addr,
            payload: Payload::Segment { batch, offset, len },
            segment_size: segment_size.unwrap_or(0),
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
            Payload::Segment { batch, offset, len } => &batch[*offset..*offset + *len],
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
            Payload::Segment { batch, .. } => {
                if Rc::strong_count(&batch) == 1 {
                    Some(batch.len())
                } else {
                    None
                }
            }
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
