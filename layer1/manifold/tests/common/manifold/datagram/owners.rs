use std::{cell::Cell, collections::VecDeque, net, rc::Rc};

use dope_core::driver::{route::KeyTag, schedule::clock};
use dope_manifold::network::datagram::{
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

pub struct Reinstallation<'a, 'd> {
    pub addr: net::SocketAddr,
    pub queued: &'a mut Vec<owners::Queued<'d, KeyTag<0>>>,
    pub flights: &'a mut Vec<owners::Flight<'d, KeyTag<0>>>,
    pub current: Owners<'d>,
    pub settled: Gate,
}

impl<'d> handler::State for Reinstallation<'_, 'd> {
    type Hooks = <FailingSend<'d> as handler::State>::Hooks;
    type Mode = ingress::Time;
}

impl handler::Progress for Reinstallation<'_, '_> {
    fn is_runnable(&self) -> bool {
        self.current.queued.is_none() && self.current.flight.is_none()
    }
}

impl handler::Progress for FailingSend<'_> {
    fn is_runnable(&self) -> bool {
        !self.pending.is_empty() && self.active.queued.is_none() && self.active.flight.is_none()
    }
}
