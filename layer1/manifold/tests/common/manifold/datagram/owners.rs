use std::{cell::Cell, collections::VecDeque, net, rc::Rc};

use dope_core::driver::{route::KeyTag, schedule::clock};
use dope_manifold::network::datagram::{
    Endpoint,
    hook::{self, app::Schedule, ingress},
    send::{Outcome, owners},
};
use dope_test::harness::signals::Gate;
use o3::buffer::bytes::bounded;

use crate::common::manifold::datagram::handler;

pub struct Event {
    pub addr: net::SocketAddr,
    pub outcome: Outcome,
    pub payload: bounded::Value<Vec<u8>>,
}

pub struct Owners<'d> {
    pub(crate) queued: Option<owners::Queued<'d, KeyTag<0>>>,
    pub(crate) flight: Option<owners::Flight<'d, KeyTag<0>>>,
}

pub struct FailingSend<'d> {
    pub addr: net::SocketAddr,
    pub(crate) pending: VecDeque<Vec<u8>>,
    pub(crate) active: Owners<'d>,
    pub prior: Owners<'d>,
    pub accepted: Option<clock::Moment<'d>>,
    pub settled: Gate,
    pub received: Gate,
    pub events: Rc<Cell<Vec<Event>>>,
}

impl handler::State for FailingSend<'_> {
    type Hooks = hook::Set<
        hook::Disabled,
        hook::Disabled,
        hook::Disabled,
        Schedule<hook::Enabled>,
        hook::Enabled,
    >;
    type Mode = ingress::Time;
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App<'d, H> {
    #[pin]
    #[manifold]
    pub(crate) endpoint: Endpoint<'d, KeyTag<0>, H>,
}

impl handler::Progress for FailingSend<'_> {
    fn is_runnable(&self) -> bool {
        !self.pending.is_empty() && self.active.queued.is_none() && self.active.flight.is_none()
    }
}
