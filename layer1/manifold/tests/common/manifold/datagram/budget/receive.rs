use dope_manifold::network::datagram::hook::{self, ingress};
use dope_test::harness::signals::Gate;
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
