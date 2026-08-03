use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope_core::driver::ready::CompletionWaker;
use dope_net::Transport;
use dope_net::link::raw::pool::outbound::OutboundPool;

use super::Core;
use super::close::ClosePhase;
use crate::DriverContext;
use crate::manifold::connector::app::ConnApp;
use crate::manifold::connector::source::{Action, Dialer};
use crate::manifold::connector::state::State;
use crate::manifold::env::Env;
use crate::runtime::__private::Deadline;

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

impl<'pool, 'd, const ID: u8, A, S, E> SourcePhase<'d, ID, A, S, E> for Core<'pool, 'd, ID, A, S, E>
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
        let now = driver.turn_now();
        let backoff_fired = {
            let ready = this.as_ref().backoff_key();
            let wake = CompletionWaker::from_ready(this.as_ref().get_ref().route.driver(), ready);
            matches!(
                this.as_mut()
                    .project()
                    .backoff_timer
                    .as_ref()
                    .poll(now, wake),
                Poll::Ready(())
            )
        };
        if !this.as_ref().project_ref().upstreams.has_pending() && !backoff_fired {
            return;
        }
        let cap = this.as_ref().project_ref().pool.capacity().get();
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
                        |slot, region| {
                            let lane = slot.raw() as usize;
                            let cleared = fields.egress_arena.clear(region, lane);
                            assert!(cleared, "reused connector lane must be quiescent");
                            State::<A::Conn, A::Send>::new(key, lane)
                        },
                        driver,
                    );
                    match submitted {
                        Ok(Some(slot)) => fields.upstreams.bind(key, slot),
                        Ok(None) => {
                            fields.upstreams.connect_deferred(key, now);
                            break;
                        }
                        Err(error) => {
                            fields.upstreams.kill(key);
                            fields.app.open_failed(key, error, driver);
                        }
                    }
                }
                Action::Backoff { min_retry_at } => {
                    if !this.as_ref().project_ref().backoff_timer.is_armed() {
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
        self.project().backoff_timer.as_ref().arm(deadline, wake);
    }

    fn arm_liveness(self: Pin<&mut Self>, deadline: Instant) {
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        self.project().liveness_timer.as_ref().arm(deadline, wake);
    }

    fn earliest_liveness(self: Pin<&Self>, timeout: Duration) -> Option<Instant> {
        let this = self.project_ref();
        let capacity = this.pool.capacity();
        let mut earliest = None;
        for idx in capacity.slots() {
            let Some(slot) = this.pool.get(idx) else {
                continue;
            };
            if !slot.state.establish.is_done() || slot.state.retired {
                continue;
            }
            if let Some(seen) = slot.state.last_recv {
                let deadline = Deadline::after(seen, timeout);
                earliest =
                    Some(earliest.map_or(deadline, |current: Instant| current.min(deadline)));
            }
        }
        earliest
    }

    fn poll_liveness(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        if !self.as_ref().project_ref().liveness_timer.is_armed() {
            return;
        }
        let now = driver.turn_now();
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        if self
            .as_mut()
            .project()
            .liveness_timer
            .as_ref()
            .poll(now, wake)
            .is_pending()
        {
            return;
        }
        let Some(timeout) = self.as_ref().project_ref().app.inbound_idle_timeout() else {
            return;
        };
        let capacity = self.as_ref().project_ref().pool.capacity();
        for idx in capacity.slots() {
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
