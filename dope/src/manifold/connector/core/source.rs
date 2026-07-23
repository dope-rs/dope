use std::pin::Pin;
use std::time::{Duration, Instant};

use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::token::SlotIndex;
use dope_net::Transport;

use super::Core;
use super::close::ClosePhase;
use crate::DriverContext;
use crate::manifold::connector::app::ConnApp;
use crate::manifold::connector::source::{Action, Dialer};
use crate::manifold::connector::state::State;
use crate::manifold::env::Env;

pub(super) trait SourcePhase<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn poll_source(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn arm_backoff(self: Pin<&mut Self>, deadline: Instant);

    fn arm_liveness(self: Pin<&mut Self>, deadline: Instant);

    fn earliest_liveness(self: Pin<&Self>, timeout: Duration) -> Option<Instant>;

    fn poll_liveness(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);
}

impl<'d, const ID: u8, A, S, E> SourcePhase<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn poll_source(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        if *this.as_ref().project_ref().draining {
            return;
        }
        let backoff_fired = {
            let fields = this.as_ref().project_ref();
            fields
                .backoff_timer
                .is_some_and(|ticket| fields.timer.is_fired(ticket))
        };
        if !this.as_ref().project_ref().upstreams.has_pending() && !backoff_fired {
            return;
        }
        let now = driver.turn_now();
        if backoff_fired {
            let fields = this.as_mut().project();
            if let Some(ticket) = fields.backoff_timer.take() {
                fields.timer.cancel(ticket);
            }
        }
        let cap = this.as_ref().project_ref().pool.capacity();
        for _ in 0..cap {
            let action = this.as_mut().project().upstreams.poll_connect(now);
            match action {
                Action::Connect { key } => {
                    let fields = this.as_mut().project();
                    let Some(socket_params) = fields.upstreams.socket_params(key) else {
                        fields.upstreams.connect_outcome(key, false, now);
                        continue;
                    };
                    let submitted = fields.pool.submit_socket_with_state(
                        socket_params,
                        |slot| {
                            State::<A::Conn, A::Send>::new(
                                key,
                                slot.raw() as usize,
                                fields.egress_arena,
                            )
                        },
                        driver,
                    );
                    match submitted {
                        Some(slot) => fields.upstreams.bind(key, slot),
                        None => {
                            fields.upstreams.connect_deferred(key, now);
                            break;
                        }
                    }
                }
                Action::Backoff { min_retry_at } => {
                    if this.as_ref().project_ref().backoff_timer.is_none() {
                        this.as_mut().arm_backoff(min_retry_at);
                    }
                    break;
                }
                Action::Idle => break,
            }
        }
    }

    fn arm_backoff(self: Pin<&mut Self>, deadline: Instant) {
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        let this = self.project();
        if let Some(ticket) = this.backoff_timer.take() {
            this.timer.cancel(ticket);
        }
        *this.backoff_timer = this.timer.try_arm(deadline, wake);
    }

    fn arm_liveness(self: Pin<&mut Self>, deadline: Instant) {
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        let this = self.project();
        if let Some(ticket) = this.liveness_timer.take() {
            this.timer.cancel(ticket);
        }
        *this.liveness_timer = this.timer.try_arm(deadline, wake);
    }

    fn earliest_liveness(self: Pin<&Self>, timeout: Duration) -> Option<Instant> {
        let this = self.project_ref();
        let cap = this.pool.capacity() as u32;
        let mut earliest = None;
        for raw in 0..cap {
            let idx = SlotIndex::new(raw);
            let Some(slot) = this.pool.get(idx) else {
                continue;
            };
            if !slot.state.establish.is_done() || slot.state.retired {
                continue;
            }
            if let Some(seen) = slot.state.last_recv {
                let deadline = seen + timeout;
                earliest =
                    Some(earliest.map_or(deadline, |current: Instant| current.min(deadline)));
            }
        }
        earliest
    }

    fn poll_liveness(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fired = {
            let fields = self.as_ref().project_ref();
            fields
                .liveness_timer
                .is_some_and(|ticket| fields.timer.is_fired(ticket))
        };
        if !fired {
            return;
        }
        {
            let this = self.as_mut().project();
            if let Some(ticket) = this.liveness_timer.take() {
                this.timer.cancel(ticket);
            }
        }
        let Some(timeout) = self.as_ref().project_ref().app.inbound_idle_timeout() else {
            return;
        };
        let now = driver.turn_now();
        let cap = self.as_ref().project_ref().pool.capacity() as u32;
        for raw in 0..cap {
            let idx = SlotIndex::new(raw);
            let expired = {
                let this = self.as_ref().project_ref();
                this.pool.get(idx).is_some_and(|slot| {
                    slot.state.establish.is_done()
                        && !slot.state.retired
                        && slot
                            .state
                            .last_recv
                            .is_some_and(|seen| now.duration_since(seen) >= timeout)
                })
            };
            if expired {
                Self::close_slot(self.as_mut(), idx, driver);
            }
        }
        if let Some(deadline) = self.as_ref().earliest_liveness(timeout) {
            self.arm_liveness(deadline);
        }
    }
}
