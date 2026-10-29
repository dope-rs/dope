use dope_core::driver::{route::Identity, storage};
use dope_net::link::pool::change;
use session::ingress;

use crate::{
    app::env,
    client::{app::request::Disabled, session, session::policy},
    network::client::topology::{factory::Built, plan},
};

/// Builds one session that has no outbound request source.
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
        + session::Session<'d, ID, Requests = Disabled>
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
