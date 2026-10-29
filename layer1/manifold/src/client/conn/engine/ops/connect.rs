use std::pin;

use attempt::Report as _;
use dope_core::{
    driver::{retained, route::Identity, schedule},
    io::event::net::{connect, socket},
};
use dope_net::{link::event, wire};
use engine::ops::{close::ClosePhase as _, phase::Phase as _};

use crate::{
    app::env,
    client::{
        app, attempt,
        close::Reason,
        conn::engine::{
            self,
            liveness::{self, refresh},
            ops::{close, fail, opens, phase, retire},
            transfer::send,
        },
        side::mode::Mode,
    },
};

/// Executes socket-open and connection-completion transitions for one engine shape.
pub(in crate::client) trait Connect<'d, ID: Identity, A, S, E>:
    opens::Phase<'d, ID, A, S, E, A::Deferred, <A::Wire as wire::Spec>::OpenFailure>
where
    A: app::Receive<'d, ID> + app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn socket(
        self: pin::Pin<&mut Self>,
        done: socket::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn try_connect(
        self: pin::Pin<&mut Self>,
        done: connect::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>>;
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Connect<'d, ID, A, S, E>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    S::Error: attempt::Report<'d, ID, A>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Failure: fail::Fail<'d, ID, A, S, E::Transport>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    Self: phase::Phase<'d, ID, A, S, E, A::Connect>
        + close::ClosePhase<'d, ID, A, S, E>
        + opens::Phase<'d, ID, A, S, E, A::Deferred, <A::Wire as wire::Spec>::OpenFailure>,
{
    fn socket(
        mut self: pin::Pin<&mut Self>,
        done: socket::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let step = {
            let this = self.as_mut().project();
            this.links.pool_mut().finish_open(done, driver, |slot| {
                (slot.state.owner.attempt(), slot.state.options)
            })
        };
        if let event::Socket::Failed {
            key,
            attempt: owner,
            cause,
        } = step
        {
            let this = self.as_mut().project();
            <A::Failure as fail::Fail<'d, ID, A, S, E::Transport>>::fail(
                this.controller,
                this.app,
                owner,
                cause,
                driver,
            );
            retire::RetirePhase::fail(self, key, turn, driver);
        }
    }

    fn try_connect(
        mut self: pin::Pin<&mut Self>,
        done: connect::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>> {
        let now = driver.driver_ref().clock().now();
        let step = self.as_mut().try_finish(done, driver)?;
        let (conn, owner, peer) = match step {
            event::Connect::Connected {
                key,
                attempt: owner,
                peer,
            } => (key, owner, peer),
            event::Connect::Failed {
                key,
                attempt: owner,
                cause,
            } => {
                let this = self.as_mut().project();
                <A::Failure as fail::Fail<'d, ID, A, S, E::Transport>>::fail(
                    this.controller,
                    this.app,
                    owner,
                    cause,
                    driver,
                );
                retire::RetirePhase::fail(self, key, turn, driver);
                return Ok(());
            }
            event::Connect::Cancelled { key } => {
                self.as_mut()
                    .close_slot(key, Reason::Transport, turn, driver);
                return Ok(());
            }
        };
        let disp = phase::Phase::connected(
            self.as_mut(),
            conn,
            owner,
            peer,
            now,
            turn.reborrow(),
            driver,
        );
        match disp {
            phase::Disposition::Active => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::drain(
                    self.as_mut(),
                    conn,
                    turn.reborrow(),
                    driver,
                );
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                    self.as_mut(),
                    conn,
                    turn.reborrow(),
                    driver,
                );
                refresh::run(self.as_mut(), conn, now, driver);
            }
            phase::Disposition::Missing => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                    self.as_mut(),
                    conn,
                    turn.reborrow(),
                    driver,
                );
                refresh::run(self.as_mut(), conn, now, driver);
            }
            phase::Disposition::Closing => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                    self.as_mut(),
                    conn,
                    turn.reborrow(),
                    driver,
                );
            }
            phase::Disposition::Discarded => {}
            phase::Disposition::Stale => return Ok(()),
            phase::Disposition::Rejected(cause) => {
                let this = self.as_mut().project();
                cause.report(
                    attempt::Completion::reported(owner, this.controller),
                    this.app,
                    driver,
                );
                self.as_mut()
                    .close_slot(conn, Reason::Transport, turn, driver);
                return Ok(());
            }
            phase::Disposition::Failed(error) => {
                let this = self.as_mut().project();
                <A::Failure as fail::Fail<'d, ID, A, S, E::Transport>>::fail(
                    this.controller,
                    this.app,
                    owner,
                    event::ConnectError::State(error),
                    driver,
                );
                retire::RetirePhase::fail(self.as_mut(), conn, turn, driver);
                return Ok(());
            }
        }
        self.as_mut().maybe_close(conn, turn, driver);
        Ok(())
    }
}
