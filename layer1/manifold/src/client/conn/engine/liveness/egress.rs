use dope_core::driver::route::Identity;
use dope_net::link::{egress, pool::pending};

use crate::{
    app::env,
    client::{self, app, conn::engine, side::mode::Mode},
};

pub(in crate::client::conn::engine) struct Egress<'a, 'p, 'w, 'd: 'p + 'w, ID: Identity, A, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(in crate::client::conn::engine) flights: &'a crate::Flights<'d, ID>,
    pub(in crate::client::conn::engine) connection:
        engine::Transition<'a, 'd, ID, E, X, &'a mut engine::Slot<'w, 'd, ID, A>>,
    pub(in crate::client::conn::engine) pending: pending::Handle<'a, 'd, ID>,
    pub(in crate::client::conn::engine) queue:
        egress::Queue<'a, 'p, 'd, { client::IOV_CAP }, A::Send>,
}
