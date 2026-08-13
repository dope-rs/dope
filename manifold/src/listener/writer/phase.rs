use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::{
    pool::{self, pending},
    slot::send,
};

use crate::listener::{self, handler, runtime::lifecycle, writer::flow};

pub(in crate::listener) trait Phase<'d, const ID: u8, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn flush_dirty(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn commit_chunk(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn maybe_close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> Phase<'d, ID, A, E> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn flush_dirty(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let n = pending::Pending::of(self.as_ref().project_ref().owner.pool()).len();
        for _ in 0..n {
            if !turn.reborrow().maintenance().take() {
                break;
            }
            let (key, work) = {
                let this = self.as_mut().project();
                let Some(scheduled) = pending::Pending::of(this.owner.pool()).pop() else {
                    break;
                };
                scheduled
            };
            if work.contains(pending::Action::Close) {
                lifecycle::Lifecycle::close_slot(self.as_mut(), key, turn.reborrow(), driver);
                continue;
            }
            if work.contains(pending::Action::Egress) {
                self.as_mut().commit_chunk(key, turn.reborrow(), driver);
            }
        }
    }

    fn commit_chunk(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let handoff = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights,
                connection: slot,
                queue: mut egress,
            }) = this.owner.pool_mut().egress_mut(key)
            else {
                return;
            };
            if slot.is_closing() {
                flow::Handoff {
                    armed_send: false,
                    restage: false,
                }
            } else {
                let deferred = egress.total_bytes() != 0;
                let flow = flow::SlotFlow::flow(slot, deferred);
                match flow {
                    flow::Flow::Inflight => flow::Handoff {
                        armed_send: false,
                        restage: false,
                    },
                    flow::Flow::Plain => {
                        flow::SlotFlow::resume_send(slot, flights, driver);
                        let deferred = egress.total_bytes() != 0;
                        flow::SlotFlow::after_handoff(slot, deferred)
                    }
                    flow::Flow::Stalled | flow::Flow::Held | flow::Flow::Clear => {
                        slot.sending().flush_pending(flights, driver);
                        flow::Handoff {
                            armed_send: false,
                            restage: false,
                        }
                    }
                    flow::Flow::Deferred => {
                        match slot.sending().submit_egress(&mut egress, flights, driver) {
                            Ok(send::Progress::Runnable) => flow::Handoff {
                                armed_send: false,
                                restage: true,
                            },
                            Ok(send::Progress::Waiting) => flow::Handoff {
                                armed_send: slot.send_status().inflight(),
                                restage: false,
                            },
                            Ok(send::Progress::Quiescent) => flow::Handoff {
                                armed_send: false,
                                restage: false,
                            },
                            Err(_) => {
                                slot.abort();
                                flow::Handoff {
                                    armed_send: false,
                                    restage: false,
                                }
                            }
                        }
                    }
                }
            }
        };
        if handoff.armed_send {
            let armed = self
                .as_mut()
                .project()
                .schedule
                .send
                .arm(key, driver.turn_now());
            if !armed {
                lifecycle::Lifecycle::close_slot(self.as_mut(), key, turn.reborrow(), driver);
                return;
            }
        }
        if handoff.restage {
            let this = self.as_mut().project();
            if let Some(handle) = pending::Pending::of(this.owner.pool()).at(key) {
                handle.mark(pending::Action::Egress);
            }
        }
        self.maybe_close_slot(key, turn, driver);
    }

    fn maybe_close_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        enum Step {
            Close,
            Retry,
            Idle,
        }
        let step = {
            use crate::listener::connection;

            let this = self.as_ref().project_ref();
            let Some((slot, queued_bytes)) = this.owner.pool().egress(key) else {
                return;
            };
            let defer = A::defer_close(this.app, connection::Ref::new(slot));
            if slot.is_closing() {
                if slot.should_close(defer) {
                    Step::Close
                } else {
                    Step::Idle
                }
            } else {
                let deferred = queued_bytes != 0;
                match flow::SlotFlow::flow(slot, deferred) {
                    flow::Flow::Plain | flow::Flow::Deferred => Step::Retry,
                    flow::Flow::Inflight | flow::Flow::Stalled | flow::Flow::Held => Step::Idle,
                    flow::Flow::Clear if slot.should_close(defer) => Step::Close,
                    flow::Flow::Clear => Step::Idle,
                }
            }
        };
        match step {
            Step::Close => lifecycle::Lifecycle::close_slot(self.as_mut(), key, turn, driver),
            Step::Retry => {
                let this = self.as_mut().project();
                if let Some(handle) = pending::Pending::of(this.owner.pool()).at(key) {
                    handle.mark(pending::Action::Egress);
                }
            }
            Step::Idle => {}
        }
    }
}
