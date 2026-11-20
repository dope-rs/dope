use dope_core::driver::{route::Identity, storage};
use dope_net::link::pool::change;
use session::ingress;

use crate::{
    app::env,
    client::{session, session::policy},
    network::client::topology::plan,
};

/// Construction returns the session/source pair and the matching prepared wire together.
type Built<'d, S, I, W, E> = Result<(session::Setup<'d, S, I>, W), E>;

/// Builds one protocol session and wire endpoint from exact driver storage.
pub trait Application<'life, 'p, 'd, S, ID: Identity + 'd, const MAX: usize, E>: Sized
where
    S: storage::Plan + 'd,
    E: env::Spec,
    E::Wire: 'd,
{
    type Input: plan::Input;
    type Session: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + policy::Compatible<'d, 'd, E::Wire, ID>
        + plan::Staged<'d, ID, <Self::Input as plan::Input>::Staging>;
    type Source: ingress::Source<'d, Authority = <Self::Input as plan::Input>::Authority> + 'life;
    type Wire: change::Prepare<'d, 'd, ID, E::Wire>;
    type Error;

    fn build(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Built<'d, Self::Session, Self::Source, Self::Wire, Self::Error>
    where
        Self: 'd,
        'd: 'p,
        'p: 'life;
}
