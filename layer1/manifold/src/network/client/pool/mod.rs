use std::{net, pin};

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{link::pool::change, tcp, wire};
use dope_runtime::{client, topology::lending};
use o3::mem::quota;

use crate::{
    app::{creation, dispatch, env},
    client::{
        app::Wake,
        conn, session,
        session::{ingress::Source, policy},
        side,
        side::{mode::Mode, startup},
    },
    network::client::{Config, Engine, Seed},
    service,
    service::{discover, sync},
    timing,
};

/// Connection owner for one endpoint set validated during installation.
#[pin_project::pin_project]
pub struct Pool<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX: usize,
    N,
    R,
    I = net::SocketAddr,
    E = env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>,
    const ENDPOINTS: usize = 16,
    X = side::Disabled,
> where
    N: session::Target<'d, ID, MAX>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
{
    #[pin]
    engine: Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>,
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, X>
    dispatch::raw::Projection<'d> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
{
    type Inner = Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>;

    fn inner(self: pin::Pin<&Self>) -> pin::Pin<&Self::Inner> {
        self.project_ref().engine
    }

    fn inner_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Inner> {
        self.project().engine
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, X>
    dispatch::raw::Install<'d> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
{
    fn install(self: pin::Pin<&mut Self>) {
        self.project().engine.as_ref().wake_control();
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, X>
    Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
{
    pub(super) fn try_with<S, W, C, O>(
        setup: session::Setup<'d, N, S>,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        config: Config<'d, W>,
        aux: X,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'a, 'domain, 'pool> FnOnce(
            Pool<'a, 'domain, 'pool, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        S: Source<'d>,
        X: startup::Startup<'d, ID>,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        config.build::<MAX, N, R, I, E, ENDPOINTS, discover::Silent, X, S, ID, C, O>(
            setup,
            Seed {
                feedback: discover::Silent,
                snapshot: Some(set),
            },
            aux,
            owner,
            context,
            |engine, owner| build(Pool { engine }, owner),
        )
    }

    pub fn session(&self) -> &N {
        self.engine.app.session()
    }

    pub fn session_mut(self: pin::Pin<&mut Self>) -> &mut N {
        self.project().engine.project().app.session_mut()
    }

    pub fn with_conn<T>(
        self: pin::Pin<&mut Self>,
        conn: conn::Id<'d, ID>,
        apply: impl FnOnce(&mut N::ConnState) -> T,
    ) -> Option<T> {
        self.project()
            .engine
            .with_conn(conn, |state| apply(state.state_mut()))
    }

    pub fn wake(self: pin::Pin<&mut Self>, conn: conn::Id<'d, ID>) -> Wake {
        self.project().engine.wake_conn(conn)
    }

    pub(super) fn maintain<'turn>(mut self: pin::Pin<&mut Self>, turn: schedule::Turn<'turn, 'd>) {
        let work = turn.maint();
        loop {
            let step = work.claim(|| {
                let mut engine = self.as_mut().project().engine;
                let ctl = engine.as_mut().control_mut();
                ctl.next_retirement()
            });
            match step {
                quota::Admission::Item((lane, permit)) => {
                    self.as_mut()
                        .project()
                        .engine
                        .retire(lane, permit, service::Pool::is_retired)
                }
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            }
        }
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, X>
    lending::Root<'d> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + client::Provider<'d> + Unpin,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
{
    type Provider = N;

    fn provider(self: pin::Pin<&Self>) -> pin::Pin<&Self::Provider> {
        pin::Pin::new(self.get_ref().session())
    }
}

mod raw;
