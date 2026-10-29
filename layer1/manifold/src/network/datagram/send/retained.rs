use std::{net, pin};

use dope_core::driver::{route::Identity, schedule::clock};
use o3::{base::cell::region, buffer::resident};

use crate::network::datagram::{self, hook, packet, send, send::kernel};

pub(in crate::network::datagram) struct Retained<'d, B> {
    pub(in crate::network::datagram) value: kernel::Payload<'d, B>,
    charge: Option<resident::Charge<'d>>,
}

impl<'d, B> Retained<'d, B> {
    pub(in crate::network::datagram) const fn new(
        value: kernel::Payload<'d, B>,
        charge: Option<resident::Charge<'d>>,
    ) -> Self {
        Self { value, charge }
    }

    pub(in crate::network::datagram) fn retire<'turn, ID: Identity, H>(
        self,
        owner: send::Owner<'d, ID>,
        addr: net::SocketAddr,
        outcome: send::Outcome,
        at: clock::Moment<'d>,
        handler: pin::Pin<&'turn mut H>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) where
        H: datagram::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        drop(self.charge);
        let payload = match self.value {
            kernel::Payload::Owned(payload) => send::Payload::Owned(payload),
            kernel::Payload::Buffer(payload) => send::Payload::Buffer(payload),
            kernel::Payload::Packet(payload) => send::Payload::Packet(packet::Packet::new(payload)),
        };
        handler.settle(
            send::Settlement::new(owner, addr, outcome, payload, at),
            token,
        );
    }
}
