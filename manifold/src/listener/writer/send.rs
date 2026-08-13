use std::pin;

use dope_core::{
    driver::{retained, schedule},
    io::event::send,
};
use dope_net::link::pool;

use crate::listener::{
    self, handler,
    runtime::lifecycle,
    writer::{flow::SlotFlow as _, phase::Phase as _},
};

pub(in crate::listener) trait SendPhase<'d, const ID: u8, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn arm_send_deadline(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn pump_send(
        self: pin::Pin<&mut Self>,
        completion: send::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> SendPhase<'d, ID, A, E> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn arm_send_deadline(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let armed = {
            let this = self.as_mut().project();
            let inflight = this
                .owner
                .pool()
                .get(key)
                .is_some_and(|slot| slot.send_status().inflight());
            !inflight || this.schedule.send.arm(key, driver.turn_now())
        };
        if !armed {
            lifecycle::Lifecycle::close_slot(self, key, turn, driver);
        }
    }

    fn pump_send(
        mut self: pin::Pin<&mut Self>,
        completion: send::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        use dope_net::link::event::SendOutcome;
        let mut this = self.as_mut().project();
        let outcome = this
            .owner
            .pool_mut()
            .ingress()
            .classify_send(driver, completion);
        let (key, completion) = match outcome {
            SendOutcome::Sent(completion) => (completion.key(), completion),
            SendOutcome::Close(completion) => {
                let key = completion.key();
                if let Some(mut egress) = this.owner.egress_mut(key) {
                    egress
                        .connection
                        .sending()
                        .abort_egress(&mut egress.queue, completion);
                }
                lifecycle::Lifecycle::close_slot(self.as_mut(), key, turn.reborrow(), driver);
                return;
            }
            SendOutcome::Drop => return,
        };
        this.schedule.send.cancel(key);
        enum Next {
            Commit,
            More(bool),
            Done,
            Close,
        }
        let next = {
            let Some(mut egress) = this.owner.egress_mut(key) else {
                return;
            };
            if egress.connection.state.send.is_queue_path() {
                match egress.connection.sending().complete_egress(
                    &mut egress.queue,
                    driver.region_token(),
                    completion,
                ) {
                    Ok(sent) => {
                        A::send(
                            this.app.as_mut(),
                            egress.context(turn.reborrow().application()),
                            sent,
                            driver,
                        );
                        Next::Commit
                    }
                    Err(_) => Next::Close,
                }
            } else {
                use dope_net::wire::{self, reclaim};

                let completed =
                    <<A::Wire as wire::Wire>::Reclaim as reclaim::Policy>::completed_plain(
                        completion.sent(),
                    );
                if completed
                    .is_some_and(|sent| !egress.connection.state.send.complete_handoff(sent))
                {
                    Next::Close
                } else if egress.connection.state.send.has_remaining() {
                    egress.connection.resume_send(egress.flights, driver);
                    Next::More(egress.connection.send_status().inflight())
                } else {
                    match egress.connection.state.send.finish() {
                        Some(total) => {
                            debug_assert!(
                                !egress.connection.send_status().inflight(),
                                "direct buffer reused while a SEND SQE is still in flight"
                            );
                            A::send(
                                this.app.as_mut(),
                                egress.context(turn.reborrow().application()),
                                total,
                                driver,
                            );
                            Next::Done
                        }
                        None => {
                            egress.connection.abort();
                            Next::Close
                        }
                    }
                }
            }
        };
        match next {
            Next::Commit => self.as_mut().commit_chunk(key, turn, driver),
            Next::More(true) => {
                if !self
                    .as_mut()
                    .project()
                    .schedule
                    .send
                    .arm(key, driver.turn_now())
                {
                    lifecycle::Lifecycle::close_slot(self, key, turn, driver);
                }
            }
            Next::More(false) => {}
            Next::Done => self.as_mut().maybe_close_slot(key, turn, driver),
            Next::Close => lifecycle::Lifecycle::close_slot(self, key, turn, driver),
        }
    }
}
