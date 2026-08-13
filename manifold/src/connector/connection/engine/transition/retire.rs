//! Retirement transition for connector-owned connections.

use std::pin;

use close::Close as _;
use dope_core::driver::{retained, schedule};
use dope_net::link::pool::{self, transition::close};

use crate::connector::{
    app, attempt, auxiliary,
    auxiliary::Kind as _,
    connection::{
        self,
        engine::{self, scheduling::deadline},
    },
    lifecycle,
};

pub(in crate::connector) trait RetirePhase<'d, const ID: u8, A, S, E, O>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn retire_failed(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn drain_close(
        pool: &mut engine::Pool<'d, ID, A, E, O>,
        app: &A,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> RetirePhase<'d, ID, A, S, E, X::Owner>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn retire_failed(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let reason = lifecycle::CloseReason::Transport;
        {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(key) else {
                return;
            };
            slot.abort();
            if slot.state.owner.is_auxiliary() {
                slot.state.closing.request_permanent();
            }
            slot.state.request_close(reason);
            let _ = slot.state.closing.retire(reason);
        }
        deadline::DeadlinePhase::clear_timeouts(self.as_mut(), key);
        let this = self.project();
        Self::drain_close(this.pool, this.app, key, turn, driver);
    }

    fn drain_close(
        pool: &mut engine::Pool<'d, ID, A, E, X::Owner>,
        app: &A,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        pool.close(
            key,
            turn.reborrow().maintenance(),
            driver,
            |phase, flights, slot, driver| {
                use dope_net::link::pool::transition::close::{Decision, Phase};

                match phase {
                    Phase::Prepare => {
                        if slot.state.owner.is_auxiliary() {
                            return Decision::Ready;
                        }
                        if slot.is_established() {
                            if !slot.is_aborted() && slot.sending().seal_graceful(flights, driver) {
                                return Decision::Waiting;
                            }
                            if !slot.is_aborted()
                                && !app.is_drained(connection::Ref::new(slot), driver)
                            {
                                return Decision::Runnable;
                            }
                        }
                        Decision::Ready
                    }
                    Phase::Release | Phase::Retire => Decision::Ready,
                }
            },
        );
    }
}
