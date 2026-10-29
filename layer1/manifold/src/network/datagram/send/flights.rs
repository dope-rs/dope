use std::net;

use dope_core::{
    dope_sys::io::socket::msg::{Header, Iovec},
    io::socket::{self, msg},
};

use crate::network::datagram::send::{self, kernel, retained};

pub(in crate::network::datagram) struct Backing<'d, B, M: send::Mode = send::Plain> {
    retained: retained::Retained<'d, B>,
    target: net::SocketAddr,
    addr: socket::Inet,
    iovs: [Iovec; 1],
    msg: Header,
    control: M::Flight,
}

impl<'d, B: AsRef<[u8]>, M: send::Mode> Backing<'d, B, M> {
    pub(in crate::network::datagram::send) fn new(queued: send::Queued<'d, B, M>) -> Self {
        let target = queued.addr;
        let control = M::prepare(queued.control);
        Self {
            retained: queued.retained,
            target,
            addr: socket::Inet::from_std(target),
            iovs: [Iovec::empty()],
            msg: Header::new(),
            control,
        }
    }

    pub(in crate::network::datagram::send) fn fill_message<'a>(&'a mut self) -> msg::Message<'a> {
        let Self {
            retained,
            addr,
            iovs,
            msg,
            control,
            ..
        } = self;
        let mut builder = msg::Builder::new(msg);
        builder.name(addr);
        let builder = M::attach(control, builder);
        let payload: &'a mut kernel::Payload<'d, B> = &mut retained.value;
        match payload {
            kernel::Payload::Owned(payload) => builder.finish(iovs, payload),
            kernel::Payload::Buffer(payload) => builder.finish(iovs, payload),
            kernel::Payload::Packet(payload) => builder.finish(iovs, payload),
        }
    }

    pub(in crate::network::datagram::send) fn into_queued(self) -> send::Queued<'d, B, M> {
        send::Queued {
            addr: self.target,
            retained: self.retained,
            control: M::restore(self.control),
        }
    }

    pub(in crate::network::datagram) fn finish(
        self,
    ) -> (net::SocketAddr, retained::Retained<'d, B>) {
        (self.target, self.retained)
    }
}
