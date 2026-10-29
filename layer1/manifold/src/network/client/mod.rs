use std::pin;

pub use config::Config;
use config::Seed;
use dope_core::driver::{
    self,
    route::Identity,
    schedule::{self, maint},
};
use dope_net::{link::pool::change, tcp, wire};
use dope_runtime::{client, topology::lending};
use o3::mem::quota;
pub use pool::Pool;
pub use prepared::Prepared;
use session::{ingress::Source, policy};
use work::{Task, Work, WorkGraph};

use crate::{
    app::{creation, dispatch, env},
    client::{
        app::Wake,
        conn::{Id, engine},
        session,
        session::app::State,
        side,
    },
    service::{self, discover, sync},
    timing,
};

type Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, const ENDPOINTS: usize, D, X> = engine::Machine<
    'p,
    'w,
    'd,
    ID,
    State<'w, 'd, ID, N, <E as env::Spec>::Wire>,
    service::Pool<'q, 'h, 'd, ID, <E as env::Spec>::Transport, R, I, ENDPOINTS, D>,
    E,
    X,
>;

mod config;
mod pool;
mod prepared;
pub mod topology;
mod work;

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, S>
    Connector<'q, 'h, 'p, 'w, 'd, ID, MAX, N, discover::Watched<S>, R, I, (), E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX>,
    S: discover::Demand<'d, Endpoint = I>,
    R: sync::Policy,
    I: Eq,
    E: env::Spec,
    E::Transport: dope_net::Transport,
{
    pub(crate) fn try_watch<Q: Source<'d>, W, C, F>(
        setup: session::Setup<'d, N, Q>,
        source: S,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        config: Config<'d, W>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'a, 'domain, 'pool> FnOnce(
            Connector<
                'a,
                'domain,
                'pool,
                'w,
                'd,
                ID,
                MAX,
                N,
                discover::Watched<S>,
                R,
                I,
                (),
                E,
                ENDPOINTS,
            >,
            &mut C,
        ) -> F,
    ) -> Result<F, creation::Error>
    where
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let feedback = discover::Gate::new(source);
        config.build::<MAX, N, R, I, E, ENDPOINTS, _, side::Disabled, Q, ID, C, F>(
            setup,
            Seed {
                feedback,
                snapshot: Some(set),
            },
            side::Disabled,
            owner,
            context,
            |engine, owner| {
                build(
                    Connector {
                        engine,
                        discovery: discover::Watched::new(),
                        observer: (),
                        work: WorkGraph::new(),
                    },
                    owner,
                )
            },
        )
    }
}

/// Discovery-driven connection owner for one logical service.
#[pin_project::pin_project]
pub struct Connector<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX_CONNECTIONS: usize,
    N,
    D,
    R,
    I,
    O,
    E = env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>,
    const ENDPOINTS: usize = 16,
> where
    N: session::Parser<'d, ID>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
{
    #[pin]
    engine: Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled>,
    discovery: D,
    observer: O,
    work: WorkGraph,
}

impl<
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
    D,
    O,
> dispatch::raw::Projection<'d>
    for Connector<'q, 'h, 'p, 'w, 'd, ID, MAX, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
{
    type Inner = Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled>;

    fn inner(self: pin::Pin<&Self>) -> pin::Pin<&Self::Inner> {
        self.project_ref().engine
    }

    fn inner_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Inner> {
        self.project().engine
    }
}

impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX_CONNECTIONS: usize,
    N,
    D,
    R,
    I,
    O,
    E,
    const ENDPOINTS: usize,
> lending::Root<'d>
    for Connector<'q, 'h, 'p, 'w, 'd, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Parser<'d, ID> + client::Provider<'d> + Unpin,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
{
    type Provider = N;

    fn provider(self: pin::Pin<&Self>) -> pin::Pin<&Self::Provider> {
        pin::Pin::new(self.get_ref().session())
    }
}

impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX_CONNECTIONS: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    D,
    O,
> Connector<'q, 'h, 'p, 'w, 'd, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Parser<'d, ID>,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
{
    pub fn session(&self) -> &N {
        self.engine.app.session()
    }

    pub fn session_mut(self: pin::Pin<&mut Self>) -> &mut N {
        self.project().engine.project().app.session_mut()
    }

    /// Applies one bounded mutation to an exact connector generation.
    pub fn with_conn<T>(
        self: pin::Pin<&mut Self>,
        conn: Id<'d, ID>,
        apply: impl FnOnce(&mut N::ConnState) -> T,
    ) -> Option<T> {
        self.project()
            .engine
            .with_conn(conn, |state| apply(state.state_mut()))
    }

    /// Wakes one exact connector generation.
    pub fn wake(self: pin::Pin<&mut Self>, conn: Id<'d, ID>) -> Wake {
        self.project().engine.wake_conn(conn)
    }

    /// Atomically reconciles desired endpoint state under the service policy.
    fn try_sync(
        mut self: pin::Pin<&mut Self>,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
    ) -> Result<service::Change, service::Reconcile> {
        let mut engine = self.as_mut().project().engine;
        let cap = engine.as_ref().capacity();
        let change = engine.as_mut().control_mut().try_sync(set, cap)?;
        engine.as_ref().wake_control();
        Ok(change)
    }

    pub(super) fn maintain<'turn>(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        mut due: bool,
        mut resolve: impl FnMut(pin::Pin<&mut Self>, maint::Permit<'turn, 'd>),
    ) {
        let work = turn.maint();
        work.share::<2, _>(|| {
            loop {
                let step = {
                    let this = self.as_mut().project();
                    let mut engine = this.engine;
                    this.work.admit(|source| match source {
                        Work::Discovery => work.claim(|| due.then_some(Task::Discovery)),
                        Work::Retirement => work.claim(|| {
                            let ctl = engine.as_mut().control_mut();
                            ctl.next_retirement().map(Task::Retirement)
                        }),
                    })
                };
                match step {
                    quota::Admission::Item((Task::Discovery, permit)) => {
                        due = false;
                        resolve(self.as_mut(), permit);
                    }
                    quota::Admission::Item((Task::Retirement(lane), permit)) => {
                        self.as_mut().project().engine.retire(
                            lane,
                            permit,
                            service::Pool::is_retired,
                        );
                    }
                    quota::Admission::Empty | quota::Admission::Exhausted => break,
                }
            }
        });
    }
}

mod cycle;

mod raw;
