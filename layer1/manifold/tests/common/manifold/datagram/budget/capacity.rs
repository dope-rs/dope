use std::net;

use dope_manifold::network::datagram::hook::{self, app::Schedule, ingress};
use dope_test::harness::signals::Gate;
use o3::buffer::storage::NonEmpty;

use crate::common::manifold::datagram::handler;

pub enum Stage {
    Initial(Vec<u8>, NonEmpty<Vec<u8>>),
    Waiting(NonEmpty<Vec<u8>>),
    Done,
}

pub struct Capacity {
    pub addr: net::SocketAddr,
    pub(crate) stage: Stage,
    pub released: Gate,
    pub received: Gate,
}

impl handler::State for Capacity {
    type Hooks = hook::Set<hook::Disabled, hook::Disabled, hook::Disabled, Schedule>;
    type Mode = ingress::Access;
}

impl handler::Progress for Capacity {
    fn is_runnable(&self) -> bool {
        matches!(self.stage, Stage::Initial(..))
    }
}
