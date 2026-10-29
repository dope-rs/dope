use dope_core::driver::{route::Identity, schedule::clock};

use crate::{
    app::env,
    client::{session, side},
    network::client::{Connector, Engine, work::WorkGraph},
    service::{discover, sync},
};

/// A complete cold connector whose discovery owner has not yet been supplied.
///
/// Its preparation scope retains the borrowed connection and service storage.
/// Attaching discovery consumes the exact engine without reopening resources.
pub struct Prepared<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
> where
    N: session::Target<'d, ID, MAX>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
{
    pub(super) engine:
        Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Notify, side::Disabled>,
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize>
    Prepared<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
{
    /// Attaches the discovery owner for the caller's admitted start time.
    pub fn discover<D: discover::Output + discover::RefreshAt<'d>, O>(
        self,
        lookup: D,
        watch: O,
        at: clock::Moment<'d>,
    ) -> Connector<'q, 'h, 'p, 'w, 'd, ID, MAX, N, discover::Refreshing<'d, D>, R, I, O, E, ENDPOINTS>
    {
        Connector {
            engine: self.engine,
            discovery: discover::Refreshing::new(lookup, at),
            observer: watch,
            work: WorkGraph::new(),
        }
    }
}
