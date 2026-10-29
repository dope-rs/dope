use dope_manifold::network::datagram::hook::{self, ingress};
use dope_test::harness::signals::Gate;

use crate::common::manifold::datagram::handler;

pub struct Packet {
    pub rejected: Gate,
}

impl handler::State for Packet {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}
