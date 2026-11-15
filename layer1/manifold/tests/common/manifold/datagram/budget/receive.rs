use std::pin::Pin;

use dope_manifold::network::datagram::{
    self,
    hook::{self, ingress},
    packet,
};
use dope_test::harness::signals::{Gate, LocalGateRef};
use o3::buffer::{
    bytes::bounded,
    storage::{Shared, provided::Account},
};

use crate::common::manifold::datagram::handler;

pub struct Receive<'d> {
    pub retained: Option<bounded::Value<Shared<Option<Account<'d>>>>>,
    pub capacity: usize,
    pub done: Gate,
}

impl handler::State for Receive<'_> {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}

pub struct Backlog<'a, 'd> {
    pub packets: packet::Backlog<'d>,
    pub received: LocalGateRef<'a>,
    pub cleared: LocalGateRef<'a>,
}

impl handler::State for Backlog<'_, '_> {
    type Hooks = hook::Set<hook::Disabled, hook::Enabled>;
    type Mode = ingress::Access;
}

impl datagram::Shutdown for Backlog<'_, '_> {
    fn shutdown(self: Pin<&mut Self>) {
        let this = self.get_mut();
        if !this.packets.is_empty() {
            this.packets.clear();
            this.cleared.hit();
        }
    }
}
