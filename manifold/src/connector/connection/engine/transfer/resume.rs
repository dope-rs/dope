use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::pool::{self, pending};

use crate::connector::{
    app::{self, continuation},
    attempt, auxiliary,
    auxiliary::Ownership as _,
    connection::{
        self,
        engine::{transfer::send, transition::close},
    },
    lifecycle,
};

pub(in crate::connector::connection::engine) trait Resume<'d, const ID: u8, A, S, E>:
    Sized
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn finish_chunk(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        outcome: <A::Continuation as continuation::Mode<'d, ID, A>>::Outcome,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        <A::Continuation as continuation::Policy<'d, ID, A>>::dispatch(
            outcome,
            (self, key, turn, driver),
            |(engine, key, turn, driver), outcome| {
                engine.finish_complete(key, outcome, turn, driver)
            },
            |(engine, key, turn, driver)| engine.finish_yield(key, turn, driver),
        )
    }

    fn finish_complete(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        outcome: app::ChunkOutcome,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome;

    fn finish_yield(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome;

    fn resume_chunk<'turn>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        permit: <A::Continuation as continuation::Policy<'d, ID, A>>::Permit<'turn>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> Resume<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn finish_complete(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        outcome: app::ChunkOutcome,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        use crate::connector::app::ChunkOutcome;

        if matches!(outcome, ChunkOutcome::Overrun) {
            return crate::Outcome::Overrun;
        }
        if matches!(outcome, ChunkOutcome::Capacity) {
            return crate::Outcome::Capacity;
        }
        send::SendPhase::submit_egress(self.as_mut(), key, turn.reborrow(), driver);
        match outcome {
            ChunkOutcome::Ok => crate::Outcome::Ok,
            ChunkOutcome::Capacity => crate::Outcome::Capacity,
            ChunkOutcome::Overrun => crate::Outcome::Overrun,
            ChunkOutcome::CloseReconnect => crate::Outcome::CloseAfter,
            ChunkOutcome::ClosePermanent => {
                let key = self
                    .as_mut()
                    .project()
                    .pool
                    .get(key)
                    .and_then(|slot| slot.state.owner.attempt());
                if let Some(key) = key {
                    self.project().controller.kill(key);
                }
                crate::Outcome::CloseAfter
            }
        }
    }

    fn finish_yield(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        let this = self.as_mut().project();
        if let Some(handle) = pending::Pending::of(this.pool).at(key) {
            handle.mark(pending::Action::Ingress);
        }
        send::SendPhase::submit_egress(self, key, turn, driver);
        crate::Outcome::Ok
    }

    fn resume_chunk<'turn>(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        permit: <A::Continuation as continuation::Policy<'d, ID, A>>::Permit<'turn>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let outcome = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                queue: egress,
            }) = this.pool.egress_mut(key)
            else {
                return;
            };
            <A::Continuation as continuation::Policy<'d, ID, A>>::resume(
                permit,
                this.app,
                connection::Ctx::new(slot, turn.reborrow().application()),
                egress,
                driver,
            )
        };
        <A::Continuation as continuation::Policy<'d, ID, A>>::dispatch(
            outcome,
            (self, key, turn, driver),
            |(mut engine, key, turn, driver), outcome| match engine.as_mut().finish_complete(
                key,
                outcome,
                turn.reborrow(),
                driver,
            ) {
                crate::Outcome::Ok => {
                    close::ClosePhase::maybe_close(engine.as_mut(), key, turn.reborrow(), driver)
                }
                crate::Outcome::Overrun => {
                    if let Some(slot) = engine.as_mut().project().pool.get_mut(key) {
                        slot.abort();
                    }
                    close::ClosePhase::close_slot(
                        engine.as_mut(),
                        key,
                        lifecycle::CloseReason::Protocol,
                        turn.reborrow(),
                        driver,
                    );
                }
                crate::Outcome::Capacity => {
                    if let Some(slot) = engine.as_mut().project().pool.get_mut(key) {
                        slot.abort();
                    }
                    close::ClosePhase::close_slot(
                        engine.as_mut(),
                        key,
                        lifecycle::CloseReason::Capacity,
                        turn.reborrow(),
                        driver,
                    );
                }
                crate::Outcome::CloseAfter => {
                    engine
                        .as_mut()
                        .project()
                        .pool
                        .ingress()
                        .set_close_after(key);
                    close::ClosePhase::maybe_close(engine.as_mut(), key, turn.reborrow(), driver);
                }
            },
            |(engine, key, turn, driver)| {
                engine.finish_yield(key, turn, driver);
            },
        );
    }
}
