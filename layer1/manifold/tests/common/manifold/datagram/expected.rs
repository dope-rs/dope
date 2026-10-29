use dope_core::{driver::route::KeyTag, io::datagram::ReceiveError};
use dope_manifold::network::datagram::{
    Endpoint,
    hook::{self, ingress},
};
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

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App<'d, H> {
    #[pin]
    #[manifold]
    pub(crate) receiver: Endpoint<'d, KeyTag<0>, H>,
}

pub struct Exhaustion<'d> {
    pub retained: Option<bounded::Value<Shared<Option<Account<'d>>>>>,
    pub done: Gate,
}

impl handler::State for Exhaustion<'_> {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}
