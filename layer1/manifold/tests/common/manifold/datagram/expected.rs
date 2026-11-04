use dope_core::io::datagram::ReceiveError;
use dope_manifold::network::datagram::hook::{self, ingress};
use dope_test::harness::signals::Gate;
use o3::buffer::{
    bytes::bounded,
    storage::{Shared, provided::Account},
};

use crate::common::manifold::datagram::handler;

pub struct Expected<'p> {
    pub gate: Gate,
    pub payload: &'p [u8],
    pub failure: Option<ReceiveError>,
}

impl handler::State for Expected<'_> {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}

pub struct Exhaustion<'d> {
    pub retained: Option<bounded::Value<Shared<Option<Account<'d>>>>>,
    pub done: Gate,
}

impl handler::State for Exhaustion<'_> {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}
