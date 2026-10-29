use std::pin;

use app::{
    admit::Dispatch as _,
    connect::{Full, Peer},
};
use dope_core::{
    driver::{retained, route::Identity, schedule, schedule::clock},
    io::{event::net::connect, socket},
};
use dope_net::link::{event, pool};
use engine::ops::close::ClosePhase as _;
use o3::mem::rc;

use crate::{
    app::env,
    client::{
        app, attempt,
        close::Reason,
        conn::{
            self, State,
            engine::{self, liveness, ops::close},
            flow, state,
        },
        deadline,
        side::mode::Mode,
    },
};

pub(super) enum Disposition<F> {
    Missing,
    Active,
    Closing,
    Discarded,
    Stale,
    Failed(rc::Error),
    Rejected(F),
}

pub(super) trait Phase<'d, ID: Identity, A, S, E, N>
where
    A: app::Receive<'d, ID> + app::Application<'d, ID, Connect = N>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    type Addr;

    fn try_finish(
        self: pin::Pin<&mut Self>,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<event::Connect<'d, ID, attempt::Id<'d, ID>, Self::Addr>, connect::Completion<'d>>;

    fn connected(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        owner: attempt::Id<'d, ID>,
        peer: Self::Addr,
        now: clock::Moment<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Disposition<S::Error>;
}

fn attach_peer<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>(
    mut engine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    key: pool::Key<'d, ID>,
    peer: A::Addr,
    now: clock::Moment<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) -> Disposition<S::Error>
where
    A: app::Lifecycle<'w, 'd, ID, Connect = Peer> + app::Peer<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    let target = engine.as_ref().backoff();
    let this = engine.as_mut().project();
    let Some(liveness::Egress {
        connection: slot,
        pending: handle,
        queue: egress,
        ..
    }) = this.links.egress(key, target)
    else {
        return Disposition::Missing;
    };
    let slot = if let Some(at) = this
        .configuration
        .connection_lifetime
        .map(|ttl| now.clamp_add(ttl))
    {
        slot.arm_at(deadline::Fixed::Lifetime, at)
    } else {
        slot.into_source()
    };
    let Some(opening) = state::OpeningMut::new(&mut *slot) else {
        return Disposition::Active;
    };
    let conn = match this.app.try_conn() {
        Ok(conn) => conn,
        Err(error) => return Disposition::Failed(error),
    };
    match opening.attach(conn, now, |active| {
        app::Peer::connected(
            this.app,
            peer,
            conn::Ctx::new(active, turn.app()),
            egress.reborrow(),
            driver,
        );
    }) {
        state::Attachment::Active => Disposition::Active,
        state::Attachment::Closing => {
            handle.wake();
            Disposition::Closing
        }
        state::Attachment::Discarded => Disposition::Discarded,
        state::Attachment::Sealed => Disposition::Closing,
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Phase<'d, ID, A, S, E, Full>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID, Connect = Full> + app::Connected<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    Self: close::ClosePhase<'d, ID, A, S, E>,
{
    type Addr = socket::Addr;

    fn try_finish(
        self: pin::Pin<&mut Self>,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<event::Connect<'d, ID, attempt::Id<'d, ID>, Self::Addr>, connect::Completion<'d>>
    {
        let target = self.as_ref().backoff();
        let this = self.project();
        this.links.try_finish(
            done,
            driver,
            |slot| {
                let slot = slot.cancel(deadline::Kind::Connect);
                slot.state.owner.attempt()
            },
            target,
        )
    }

    fn connected(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        owner: attempt::Id<'d, ID>,
        peer: Self::Addr,
        now: clock::Moment<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Disposition<S::Error> {
        let ctrl = &mut self.as_mut().project().controller;
        match ctrl.connected(owner) {
            Err(cause) => return Disposition::Rejected(cause),
            Ok(attempt::Connection::Applied(())) => {}
            Ok(attempt::Connection::Stale) => {
                self.as_mut()
                    .close_slot(key, Reason::Transport, turn.reborrow(), driver);
                return Disposition::Stale;
            }
            Ok(attempt::Connection::Disabled)
            | Ok(attempt::Connection::Retired)
            | Ok(attempt::Connection::Exhausted) => {
                self.as_mut()
                    .close_slot(key, Reason::EndpointRetired, turn.reborrow(), driver);
                return Disposition::Stale;
            }
        }
        if self.as_ref().project_ref().controller.is_pending() {
            self.as_ref().wake_control();
        }
        let ttl = self
            .as_ref()
            .project_ref()
            .configuration
            .connection_lifetime;
        let expiry = ttl.map(|ttl| now.clamp_add(ttl));
        let target = self.as_ref().backoff();
        let this = self.as_mut().project();
        let Some(liveness::Egress {
            connection: slot,
            pending: handle,
            queue: egress,
            ..
        }) = this.links.egress(key, target)
        else {
            return Disposition::Missing;
        };
        let slot = if let Some(at) = expiry {
            slot.arm_at(deadline::Fixed::Lifetime, at)
        } else {
            slot.into_source()
        };
        let receipt = attempt::Completion::reported(owner, this.controller);
        let start =
            conn::Activation::new(&receipt, conn::Id::from_key(key), slot.io().wake_target());
        let Some(opening) = state::OpeningMut::new(&mut *slot) else {
            return Disposition::Active;
        };
        let conn = match this.app.try_conn() {
            Ok(conn) => conn,
            Err(error) => return Disposition::Failed(error),
        };
        match A::Activation::admit(this.app, &start) {
            app::Admission::Attached => {}
            app::Admission::Rejected(rule) => {
                let slot = opening.into_slot();
                let flow = State::apply(slot, rule);
                handle.wake();
                match flow {
                    flow::Apply::Applied(flow::Flow::Continue(())) => {
                        return Disposition::Closing;
                    }
                    flow::Apply::Sealed => return Disposition::Closing,
                    flow::Apply::Applied(flow::Flow::Discarded(())) => {
                        return Disposition::Discarded;
                    }
                }
            }
        }
        match opening.attach(conn, now, |active| {
            app::Connected::connected(
                this.app,
                attempt::Completion::reported(start.attempt(), this.controller),
                peer,
                conn::Established::new(active, expiry),
                egress.reborrow(),
                driver,
            );
        }) {
            state::Attachment::Active => Disposition::Active,
            state::Attachment::Closing => {
                handle.wake();
                Disposition::Closing
            }
            state::Attachment::Discarded => Disposition::Discarded,
            state::Attachment::Sealed => Disposition::Closing,
        }
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Phase<'d, ID, A, S, E, Peer>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID, Wire = E::Wire>
        + app::Lifecycle<'w, 'd, ID, Connect = Peer>
        + app::Peer<'d, ID, Addr = S::Target>,
    S: attempt::Peer<'d, E::Transport, ID>,
    S::Target: Copy,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    Self: close::ClosePhase<'d, ID, A, S, E>,
{
    type Addr = ();

    fn try_finish(
        self: pin::Pin<&mut Self>,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<event::Connect<'d, ID, attempt::Id<'d, ID>, Self::Addr>, connect::Completion<'d>>
    {
        let target = self.as_ref().backoff();
        let this = self.project();
        this.links.try_attach(
            done,
            driver,
            |slot| {
                let slot = slot.cancel(deadline::Kind::Connect);
                slot.state.owner.attempt()
            },
            target,
        )
    }

    fn connected(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        owner: attempt::Id<'d, ID>,
        _: Self::Addr,
        now: clock::Moment<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Disposition<S::Error> {
        let ctrl = &mut self.as_mut().project().controller;
        let peer = match ctrl.attach(owner, Clone::clone) {
            attempt::Connection::Applied(peer) => peer,
            attempt::Connection::Stale => {
                self.as_mut()
                    .close_slot(key, Reason::Transport, turn.reborrow(), driver);
                return Disposition::Stale;
            }
            attempt::Connection::Disabled
            | attempt::Connection::Retired
            | attempt::Connection::Exhausted => {
                self.as_mut()
                    .close_slot(key, Reason::EndpointRetired, turn.reborrow(), driver);
                return Disposition::Stale;
            }
        };
        if self.as_ref().project_ref().controller.is_pending() {
            self.as_ref().wake_control();
        }
        attach_peer(self, key, peer, now, turn, driver)
    }
}
