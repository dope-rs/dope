use dope_core::driver::{route::Identity, settings, storage};
use dope_net::{link::pool::change, wire::domain};
use dope_runtime::topology::recipe;

use crate::{
    app::{env, receive},
    network::listener::handler,
};

type Built<'life, ID, A, W, E> = Result<(A, domain::Endpoint<'life, W, ID>), E>;

/// Lifetime-independent receive mode selected before driver construction.
pub trait ReceiveSpec {
    type Input: receive::Resources;
}

/// Builds a lifetime-branded application and wire endpoint from driver storage.
pub trait Application<S, ID: Identity, E>: ReceiveSpec + settings::Resource + Sized
where
    S: storage::Plan,
    E: env::Spec,
    E::Transport: dope_net::Listen,
{
    type Application<'life, 'p, 'd>: handler::Application<'d, ID, Wire = E::Wire, Input = Self::Input>
        + handler::Runtime<'life, 'd, ID>
        + 'life
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
    type Error;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Built<'life, ID, Self::Application<'life, 'p, 'd>, E::Wire, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
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

pub trait BinderError {
    type Error;
}

/// Binds one storage-built service to its application and wire owner.
pub trait Binder<S, R, ID: Identity, E>:
    BinderError + ReceiveSpec + settings::Resource + Sized
where
    S: storage::Plan,
    R: recipe::Build<S>,
    E: env::Spec,
    E::Transport: dope_net::Listen,
{
    type Application<'life, 'p, 'd>: handler::Application<'d, ID, Wire = E::Wire, Input = Self::Input>
        + handler::Runtime<'life, 'd, ID>
        + 'life
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd,
        Self::Input: 'd;

    fn bind<'life, 'p, 'd>(
        self,
        service: R::Output<'life, 'p, 'd>,
    ) -> Built<'life, ID, Self::Application<'life, 'p, 'd>, E::Wire, <Self as BinderError>::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
}
