use std::pin;

use dope_core::driver::{route::Identity, schedule};
use dope_net::Transport;
use o3::base::cell::region;

use crate::{
    app::env,
    client::{session, session::policy},
    network::client,
    service::{self, discover, observe, sync},
};

pub(super) trait Cycle<'d> {
    fn wait(
        self: pin::Pin<&Self>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d>;

    fn prepare<'a, 'turn>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> pin::Pin<&'a mut Self>;
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity, const MAX: usize, N, R, I, E, const ENDPOINTS: usize, S>
    Cycle<'d>
    for client::Connector<
        'q,
        'h,
        'p,
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
    >
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    S: discover::Demand<'d, Endpoint = I>,
    R: sync::Policy,
    I: Eq,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
{
    fn wait(
        self: pin::Pin<&Self>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if this.engine.control().is_stale(token) {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }

    fn prepare<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> pin::Pin<&'a mut Self> {
        let this = self.as_ref().project_ref();
        let due = this.engine.control().is_stale(token);
        self.as_mut()
            .maintain(turn.reborrow(), due, |mut owner, permit| {
                permit.run(|| {
                    let this = owner.as_mut().project();
                    let mut engine = this.engine;
                    let cap = engine.as_ref().capacity();
                    engine.as_mut().control_mut().set_demand(token, cap);
                    engine.as_ref().wake_control();
                });
            });
        self
    }
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
    O,
    E,
    const ENDPOINTS: usize,
    S,
> Cycle<'d>
    for client::Connector<
        'q,
        'h,
        'p,
        'w,
        'd,
        ID,
        MAX,
        N,
        discover::Refreshing<'d, S>,
        R,
        I,
        O,
        E,
        ENDPOINTS,
    >
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    S: discover::Output
        + discover::RefreshAt<'d>
        + discover::PollAt<
            'd,
            I,
            <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
            ENDPOINTS,
        >,
    R: sync::Policy,
    I: Eq,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    O: observe::Audit<
            'd,
            I,
            <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
            S,
            ENDPOINTS,
        > + observe::Failure<
            'd,
            I,
            <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
            S,
            ENDPOINTS,
        >,
{
    fn wait(
        self: pin::Pin<&Self>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if this.engine.control().feedback.is_ready() || this.discovery.source().is_ready(token) {
            return schedule::Progress::Runnable;
        }
        match this.discovery.poll_at() {
            Some(at) => schedule::Progress::until(at),
            None => schedule::Progress::Quiescent,
        }
    }

    fn prepare<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> pin::Pin<&'a mut Self> {
        let now = turn.clock().now();
        let this = self.as_ref().project_ref();
        let due = this.discovery.poll_at().is_some_and(|at| at <= now)
            || this.engine.control().feedback.is_ready()
            || this.discovery.source().is_ready(token);
        self.as_mut()
            .maintain(turn.reborrow(), due, |mut owner, permit| {
                permit.run(|| {
                    let cause = owner
                        .as_mut()
                        .project()
                        .engine
                        .control_mut()
                        .feedback
                        .take();
                    if let Some(cause) = cause {
                        let source = owner.as_mut().project().discovery;
                        source
                            .source_mut()
                            .refresh_at(now, discover::Reason::Failure(cause));
                        source.schedule(Some(now.into()));
                    }
                    let source = owner.as_mut().project().discovery;
                    let action = discover::PollAt::poll_at(source.source_mut(), token, now);
                    match action {
                        discover::Action::Pending { poll_at: next } => {
                            owner.project().discovery.schedule(next);
                        }
                        discover::Action::Published {
                            snapshot: set,
                            metadata: meta,
                            poll_at: next,
                        } => {
                            let rev = set.revision();
                            {
                                let this = owner.as_mut().project();
                                this.observer.resolved(this.discovery.source(), &set, &meta);
                            }
                            let result = owner.as_mut().try_sync(set);
                            let this = owner.project();
                            this.discovery.schedule(next);
                            match result {
                                Ok(change) => {
                                    this.observer
                                        .reconciled(this.discovery.source(), rev, change)
                                }
                                Err(error) => {
                                    this.observer.rejected(this.discovery.source(), error)
                                }
                            }
                        }
                        discover::Action::Stale {
                            revision: rev,
                            poll_at: next,
                        } => {
                            let this = owner.project();
                            this.discovery.schedule(next);
                            match this.engine.as_ref().control().try_retain(rev) {
                                Ok(()) => this.observer.stale(this.discovery.source(), rev),
                                Err(error) => {
                                    this.observer.rejected(this.discovery.source(), error)
                                }
                            }
                        }
                        discover::Action::Expired {
                            revision: rev,
                            poll_at: next,
                        } => {
                            let set = service::Snapshot::empty(rev);
                            {
                                let this = owner.as_mut().project();
                                this.observer.expired(this.discovery.source(), rev);
                            }
                            let result = owner.as_mut().try_sync(set);
                            let this = owner.project();
                            this.discovery.schedule(next);
                            match result {
                                Ok(change) => {
                                    this.observer
                                        .reconciled(this.discovery.source(), rev, change)
                                }
                                Err(error) => {
                                    this.observer.rejected(this.discovery.source(), error)
                                }
                            }
                        }
                        discover::Action::Failed {
                            error,
                            poll_at: next,
                        } => {
                            let this = owner.project();
                            this.observer.failed(this.discovery.source(), error);
                            this.discovery.schedule(next);
                        }
                    }
                });
            });
        self
    }
}
