use std::marker::PhantomData;

use dope_core::{driver, driver::route::Identity};
use dope_net::link::{egress, pool::change};
use o3::base::time::Positive;
use session::{app, ingress::Source};

use crate::{
    app::{creation, env},
    client::{conn::engine::Startup, session, side, side::startup},
    service::{self, discover, health, sync},
};

pub(super) struct Seed<F, I, A, const N: usize> {
    pub(super) feedback: F,
    pub(super) snapshot: Option<service::Snapshot<service::Endpoint<I, A>, N>>,
}

/// Bound client construction settings retained through installation.
pub struct Config<'d, W> {
    caps: side::Capacities,
    backoff: health::Backoff,
    wire: W,
    egress: egress::Config,
    connection_lifetime: Option<Positive>,
    _domain: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, W> Config<'d, W> {
    pub(super) const fn new(caps: side::Capacities, retry: health::Backoff, wire: W) -> Self {
        Self {
            caps,
            backoff: retry,
            wire,
            egress: egress::Config::DEFAULT,
            connection_lifetime: None,
            _domain: PhantomData,
        }
    }

    pub(super) const fn with_egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    pub(super) const fn with_ttl(mut self, ttl: Option<Positive>) -> Self {
        self.connection_lifetime = ttl;
        self
    }
}

impl<'d, W> Config<'d, W> {
    pub(super) fn build<
        'w,
        const MAX: usize,
        N,
        R,
        I,
        E,
        const ENDPOINTS: usize,
        F,
        X,
        S,
        ID: Identity + 'd,
        C,
        O,
    >(
        self,
        setup: session::Setup<'d, N, S>,
        seed: Seed<F, I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr, ENDPOINTS>,
        aux: X,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, F, X>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        'd: 'w,
        N: session::Target<'d, ID, MAX>,
        S: Source<'d>,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        F: discover::Access,
        X: startup::Startup<'d, ID>,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let Self {
            caps,
            backoff: retry,
            wire,
            egress,
            connection_lifetime: ttl,
            _domain: _,
        } = self;
        let cap = caps.primary();
        let opts = <E::Transport as dope_net::Transport>::options(Default::default());
        let Seed { feedback, snapshot } = seed;
        service::Pool::try_with(cap, retry, opts, feedback, |mut ctl| {
            if let Some(set) = snapshot {
                ctl.try_sync(set, cap.into())
                    .map_err(creation::Error::Service)?;
            }
            let app = app::State::try_new(setup, cap, &mut context(owner))?;
            Startup {
                capacities: caps,
                egress,
                wire,
                connection_lifetime: ttl,
                app,
                ctl,
                aux,
                source: (),
            }
            .try_with(owner, context, build)
        })
        .map_err(creation::Error::Pool)?
    }
}
