use dope_core::driver::{route::Identity, storage};
use dope_net::link::pool::change;
use dope_runtime::topology::recipe;
use session::{app::request::Source, ingress};

use crate::{
    app::env,
    client::{session, session::policy},
    network::client::topology::plan,
};

pub mod disabled;

/// Construction returns the session/source pair and the matching prepared wire together.
type Built<'d, S, I, W, E> = Result<(session::Setup<'d, S, I>, W), E>;

/// Builds one protocol session and wire endpoint from exact driver storage.
pub trait Application<S, ID: Identity, const MAX: usize, E>: Sized
where
    S: storage::Plan,
    E: env::Spec,
{
    type Input: plan::Input;
    type Session<'life, 'p, 'd>: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + Source<'d, ID>
        + policy::Compatible<'d, 'd, E::Wire, ID>
        + plan::Staged<'d, ID, <Self::Input as plan::Input>::Staging>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Source<'life, 'p, 'd>: ingress::Source<'d, Authority = <Self::Input as plan::Input>::Authority>
        + 'life
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Wire<'life, 'p, 'd>: change::Prepare<'d, 'd, ID, E::Wire>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Error;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Built<
        'd,
        Self::Session<'life, 'p, 'd>,
        Self::Source<'life, 'p, 'd>,
        Self::Wire<'life, 'p, 'd>,
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
}

/// Error vocabulary shared by every storage recipe a binder accepts.
pub trait BinderError {
    type Error;
}

/// Binds a recipe-produced service to one connector session.
pub trait Binder<S, R, ID: Identity, const MAX: usize, E>: BinderError + Sized
where
    S: storage::Plan,
    R: recipe::Build<S>,
    E: env::Spec,
{
    type Input: plan::Input;
    type Session<'life, 'p, 'd>: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + Source<'d, ID>
        + policy::Compatible<'d, 'd, E::Wire, ID>
        + plan::Staged<'d, ID, <Self::Input as plan::Input>::Staging>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd;
    type Source<'life, 'p, 'd>: ingress::Source<'d, Authority = <Self::Input as plan::Input>::Authority>
        + 'life
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd;
    type Wire<'life, 'p, 'd>: change::Prepare<'d, 'd, ID, E::Wire>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd;

    fn bind<'life, 'p, 'd>(
        self,
        service: R::Output<'life, 'p, 'd>,
    ) -> Built<
        'd,
        Self::Session<'life, 'p, 'd>,
        Self::Source<'life, 'p, 'd>,
        Self::Wire<'life, 'p, 'd>,
        <Self as BinderError>::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        R: 'd,
        E::Wire: 'd;
}
