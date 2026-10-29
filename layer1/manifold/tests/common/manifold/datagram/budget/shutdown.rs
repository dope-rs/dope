use std::{net, pin, rc::Rc};

use dope_manifold::network::datagram::{
    self,
    hook::{self, app::Schedule, ingress},
};
use dope_test::harness::signals::Gate;

use crate::common::{Sequence, manifold::datagram::handler};

pub const PENDING: u16 = 257;

#[derive(Clone, Copy)]
pub enum Mode {
    Queue,
    Flight,
}

pub struct State {
    pub addr: net::SocketAddr,
    pub(crate) payloads: Option<Vec<Vec<u8>>>,
    pub mode: Mode,
    pub queued: Gate,
    pub(crate) audit: Rc<Sequence>,
}

impl handler::State for State {
    type Hooks = hook::Set<hook::Disabled, hook::Enabled, hook::Disabled, Schedule, hook::Enabled>;
    type Mode = ingress::Time;
}

impl datagram::Shutdown for State {
    fn shutdown(self: pin::Pin<&mut Self>) {
        self.get_mut().audit.stop();
    }
}

impl State {
    pub fn payloads() -> Vec<Vec<u8>> {
        (0..PENDING)
            .map(|index| index.to_ne_bytes().to_vec())
            .collect()
    }
}

impl handler::Progress for State {
    fn is_runnable(&self) -> bool {
        self.payloads.is_some()
    }
}
