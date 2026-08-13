use std::{pin, time};

use close::Close as _;
use dope_core::driver::{retained, schedule};
use dope_net::link::pool::{self, transition::close};

use crate::listener::{self, handler};

pub(in crate::listener) trait Lifecycle<'d, const ID: u8, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn drain_deadline<K: listener::DeadlineKind<'d, ID>>(
        self: pin::Pin<&mut Self>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn drain_shutdown(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn abort_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> Lifecycle<'d, ID, A, E> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn drain_shutdown(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if !self.as_ref().project_ref().schedule.is_closing() {
            return;
        }
        self.as_mut().project().accept.retry_stop(driver);
        loop {
            if !turn.reborrow().maintenance().take() {
                return;
            }
            let next = {
                let this = self.as_mut().project();
                let capacity = this.owner.pool().inspection().capacity();
                this.schedule.take_shutdown(capacity)
            };
            let Some(index) = next else {
                break;
            };
            let Some(key) = self.as_ref().project_ref().owner.pool().key_at(index) else {
                continue;
            };
            self.as_mut().close_slot(key, turn.reborrow(), driver);
        }
    }

    fn abort_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if let Some(slot) = self.as_mut().project().owner.pool_mut().get_mut(key) {
            slot.abort();
        }
        self.close_slot(key, turn, driver);
    }

    fn drain_deadline<K: listener::DeadlineKind<'d, ID>>(
        mut self: pin::Pin<&mut Self>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            if turn.reborrow().maintenance().remaining() == 0 {
                break;
            }
            let expired = {
                let this = self.as_mut().project();
                K::get(this.schedule).pop_expired(now)
            };
            let Some(key) = expired else {
                break;
            };
            turn.reborrow().maintenance().take();
            self.as_mut().abort_slot(key, turn.reborrow(), driver);
        }
    }

    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let mut this = self.project();
        this.schedule.inbound.cancel(key);
        this.schedule.send.cancel(key);
        this.schedule.absolute.cancel(key);
        this.owner.pool_mut().close(
            key,
            turn.reborrow().maintenance(),
            driver,
            |phase, flights, slot, driver| {
                use close::{Decision, Phase};

                match phase {
                    Phase::Prepare => {
                        if slot.is_established()
                            && !slot.is_aborted()
                            && slot.sending().seal_graceful(flights, driver)
                        {
                            Decision::Waiting
                        } else {
                            Decision::Ready
                        }
                    }
                    Phase::Release => {
                        if slot.is_established() {
                            A::close(this.app.as_mut(), {
                                use crate::listener::connection::Ctx;

                                Ctx::sealed(slot, turn.reborrow().application())
                            });
                        }
                        if let Some(ip) = slot.state.peer_ip.take() {
                            this.accept.as_mut().release_peer_ip(ip);
                        }
                        Decision::Ready
                    }
                    Phase::Retire => {
                        slot.state.send.retire();
                        Decision::Ready
                    }
                }
            },
        );
    }
}
