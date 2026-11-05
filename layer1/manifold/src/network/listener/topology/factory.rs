use dope_core::driver::route::Identity;
use dope_net::link::pool::change;

use crate::{
    app::{env, receive},
    network::listener::handler,
};

/// Lifetime-independent receive mode selected before driver construction.
pub trait ReceiveSpec {
    type Input: receive::Resources;
}

/// Builds an application owned directly by the listener topology.
pub trait Direct<ID: Identity, E>: ReceiveSpec + Sized
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
{
    type Application<'d>: handler::Application<'d, ID, Wire = E::Wire, Input = Self::Input>
        + handler::Runtime<'d, 'd, ID>
        + 'd
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
    type Error;

    type Wire<'d>: change::Prepare<'d, 'd, ID, E::Wire>
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd,
        Self::Input: 'd;

    fn build<'d>(self) -> Result<(Self::Application<'d>, Self::Wire<'d>), Self::Error>
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
}
