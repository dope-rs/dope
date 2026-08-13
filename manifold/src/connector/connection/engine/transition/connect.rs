use std::pin;

use dope_core::{
    driver::{retained, schedule},
    io::event::{connect, creation},
};
use dope_net::link::{
    event,
    pool::{self, pending},
};

use crate::{
    connector::{
        app, attempt, auxiliary,
        auxiliary::Ownership as _,
        connection::{
            self,
            engine::{
                scheduling::deadline,
                transfer::send,
                transition::{close, retire},
            },
        },
        lifecycle,
    },
    timing,
};

pub(in crate::connector::connection::engine) trait ConnectPhase<'d, const ID: u8, A, S, E>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn socket(
        self: pin::Pin<&mut Self>,
        completion: creation::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn connect(
        self: pin::Pin<&mut Self>,
        completion: connect::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

fn fail_open<'d, const ID: u8, A, S, E, X>(
    mut engine: pin::Pin<&mut connection::Engine<'d, ID, A, S, E, X>>,
    key: pool::Key<'d, ID>,
    owner: auxiliary::Event<'d, ID>,
    cause: event::ConnectFailure,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    match owner {
        auxiliary::Event::Primary(attempt) => {
            let now = driver.turn_now();
            let this = engine.as_mut().project();
            this.app.connect_failed(attempt, cause, driver);
            this.controller.connect_failed(attempt, now);
        }
        auxiliary::Event::Auxiliary => {
            let this = engine.as_mut().project();
            if let Some(slot) = this.pool.get_mut(key) {
                this.auxiliary.settle(
                    &mut slot.state.owner,
                    Err(auxiliary::Error::Connect),
                    driver.region_token(),
                );
            }
        }
    }
    retire::RetirePhase::retire_failed(engine, key, turn, driver);
}

impl<'d, const ID: u8, A, S, E, X> ConnectPhase<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn socket(
        mut self: pin::Pin<&mut Self>,
        completion: creation::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let step = {
            let this = self.as_mut().project();
            this.pool.complete_socket(completion, driver, |slot| {
                let owner = match slot.state.owner.attempt() {
                    Some(attempt) => auxiliary::Event::Primary(attempt),
                    None => auxiliary::Event::Auxiliary,
                };
                (owner, Some(slot.state.options))
            })
        };
        if let event::Socket::Failed {
            key,
            attempt: owner,
            cause,
        } = step
        {
            fail_open(self, key, owner, cause, turn, driver);
        }
    }

    fn connect(
        mut self: pin::Pin<&mut Self>,
        completion: connect::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let now = driver.turn_now();
        let step = {
            let this = self.as_mut().project();
            this.pool.complete_connect(completion, driver, |slot| {
                match slot.state.owner.attempt() {
                    Some(attempt) => auxiliary::Event::Primary(attempt),
                    None => auxiliary::Event::Auxiliary,
                }
            })
        };
        let (connection, owner, peer) = match step {
            event::Connect::Connected { key, attempt, peer } => (key, attempt, peer),
            event::Connect::Failed {
                key,
                attempt: owner,
                cause,
            } => {
                fail_open(self, key, owner, cause, turn, driver);
                return;
            }
            event::Connect::Stale => return,
        };
        let auxiliary::Event::Primary(key) = owner else {
            send::SendPhase::submit_egress(self.as_mut(), connection, turn.reborrow(), driver);
            close::ClosePhase::maybe_close(self, connection, turn, driver);
            return;
        };
        deadline::DeadlinePhase::cancel_timeout(
            self.as_mut(),
            connection,
            lifecycle::TimeoutKind::Connect,
        );
        if matches!(
            self.as_mut()
                .project()
                .controller
                .connect_succeeded(key, now),
            attempt::Transition::Stale
        ) {
            close::ClosePhase::close_slot(
                self.as_mut(),
                connection,
                lifecycle::CloseReason::Transport,
                turn.reborrow(),
                driver,
            );
            return;
        }
        if !deadline::DeadlinePhase::arm_timeout(
            self.as_mut(),
            connection,
            lifecycle::TimeoutKind::Lifetime,
            now,
            <E::Timing as timing::Policy>::ABS_CONN_AGE,
        ) {
            return close::ClosePhase::abort_slot(
                self.as_mut(),
                connection,
                lifecycle::CloseReason::Timeout(lifecycle::TimeoutKind::Lifetime),
                turn,
                driver,
            );
        }
        let connected = {
            let this = self.as_mut().project();
            if let Some(pending::ScheduledEgress {
                flights: _,
                connection: slot,
                pending: _,
                queue: egress,
            }) = pending::Mut::of(this.pool).egress(connection)
            {
                slot.state.last_recv = Some(now);
                slot.state.peer = Some(peer);
                this.app.connected(
                    key,
                    peer,
                    connection::Ctx::new(slot, turn.reborrow().application()),
                    egress,
                    driver,
                );
                true
            } else {
                false
            }
        };
        if connected {
            let this = self.as_mut().project();
            <connection::Engine<'d, ID, A, S, E, X> as send::SendPhase<ID, A, S, E, X::Owner>>::drain_requests(this.app, this.pool, connection, turn.reborrow(), driver);
        }
        send::SendPhase::submit_egress(self.as_mut(), connection, turn.reborrow(), driver);
        deadline::DeadlinePhase::refresh_inbound(
            self.as_mut(),
            connection,
            now,
            turn.reborrow(),
            driver,
        );
        close::ClosePhase::maybe_close(self.as_mut(), connection, turn, driver);
    }
}
