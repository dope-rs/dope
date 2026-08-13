use std::pin;

use app::continuation;
use dope_core::{
    driver::{retained, schedule},
    io::event::send,
};
use dope_net::link::pool::{self, pending};

use crate::connector::{
    app, attempt, auxiliary,
    auxiliary::Kind as _,
    connection::{
        self,
        engine::{
            scheduling::deadline,
            transfer::{resume::Resume as _, send::SendPhase as _},
            transition::close,
        },
    },
    lifecycle,
};

pub(in crate::connector) trait FlushPhase<'d, const ID: u8, A, S, E>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn flush_dirty(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn handle_send(
        self: pin::Pin<&mut Self>,
        completion: send::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> FlushPhase<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn flush_dirty(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let n = pending::Pending::of(self.as_ref().project_ref().pool).len();
        for _ in 0..n {
            if !turn.reborrow().maintenance().take() {
                break;
            }
            let (key, work) = {
                let this = self.as_mut().project();
                let Some(scheduled) = pending::Pending::of(this.pool).pop() else {
                    break;
                };
                scheduled
            };
            let (engine, key, work, turn, driver) =
                <A::Continuation as continuation::Policy<'d, ID, A>>::admit(
                    work.contains(pending::Action::Ingress),
                    turn.reborrow().application(),
                    (self, key, work, turn, &mut *driver),
                    |(mut engine, key, work, turn, driver), permit| {
                        engine
                            .as_mut()
                            .resume_chunk(key, permit, turn.reborrow(), driver);
                        (engine, key, work, turn, driver)
                    },
                    |(mut engine, key, work, turn, driver)| {
                        let this = engine.as_mut().project();
                        if let Some(handle) = pending::Pending::of(this.pool).at(key) {
                            handle.mark(pending::Action::Ingress);
                        }
                        if work.contains(pending::Action::Egress) {
                            engine.as_mut().submit_egress(key, turn.reborrow(), driver);
                        }
                        (engine, key, work, turn, driver)
                    },
                    |(mut engine, key, work, turn, driver)| {
                        if work.contains(pending::Action::Egress) {
                            engine.as_mut().submit_egress(key, turn.reborrow(), driver);
                        }
                        (engine, key, work, turn, driver)
                    },
                );
            self = engine;
            if work.contains(pending::Action::Close) {
                close::ClosePhase::close_slot(
                    self.as_mut(),
                    key,
                    lifecycle::CloseReason::Local,
                    turn.reborrow(),
                    driver,
                );
            }
        }
    }

    fn handle_send(
        mut self: pin::Pin<&mut Self>,
        completion: send::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        use dope_net::link::event::SendOutcome;

        let outcome = self
            .as_mut()
            .project()
            .pool
            .ingress()
            .classify_send(driver, completion);
        let (key, completion) = match outcome {
            SendOutcome::Sent(completion) => (completion.key(), completion),
            SendOutcome::Close(completion) => {
                let key = completion.key();
                let this = self.as_mut().project();
                if let Some(pool::EgressMut {
                    flights: _,
                    connection: slot,
                    mut queue,
                }) = this.pool.egress_mut(key)
                {
                    slot.sending().abort_egress(&mut queue, completion);
                }
                return close::ClosePhase::close_slot(
                    self.as_mut(),
                    key,
                    lifecycle::CloseReason::Transport,
                    turn,
                    driver,
                );
            }
            SendOutcome::Drop => return,
        };
        let auxiliary = self
            .as_ref()
            .project_ref()
            .pool
            .get(key)
            .is_some_and(|slot| slot.state.owner.is_auxiliary());
        if !auxiliary {
            deadline::DeadlinePhase::cancel_timeout(
                self.as_mut(),
                key,
                lifecycle::TimeoutKind::Send,
            );
        }
        let mut drain_requests = false;
        let mut auxiliary_complete = false;
        {
            let this = self.as_mut().project();
            if let Some(pending::ScheduledEgress {
                flights: _,
                connection: slot,
                pending: handle,
                mut queue,
            }) = pending::Mut::of(this.pool).egress(key)
            {
                let Ok(_) =
                    slot.sending()
                        .complete_egress(&mut queue, driver.region_token(), completion)
                else {
                    close::ClosePhase::close_slot(
                        self.as_mut(),
                        key,
                        lifecycle::CloseReason::Transport,
                        turn.reborrow(),
                        driver,
                    );
                    return;
                };
                if slot.state.owner.is_auxiliary() {
                    if queue.total_bytes() == 0
                        && this.auxiliary.settle(
                            &mut slot.state.owner,
                            Ok(()),
                            driver.region_token(),
                        )
                    {
                        slot.state.closing.request_permanent();
                        slot.state.request_close(lifecycle::CloseReason::Local);
                        handle.mark(pending::Action::Close);
                        auxiliary_complete = true;
                    }
                } else {
                    this.app
                        .sent(connection::Id::from_key(key), queue.total_bytes() != 0);
                    drain_requests = true;
                }
            }
        }
        if auxiliary_complete {
            deadline::DeadlinePhase::cancel_timeout(
                self.as_mut(),
                key,
                lifecycle::TimeoutKind::Auxiliary,
            );
        }
        if drain_requests {
            let this = self.as_mut().project();
            Self::drain_requests(this.app, this.pool, key, turn.reborrow(), driver);
        }
        self.as_mut().submit_egress(key, turn.reborrow(), driver);
        close::ClosePhase::maybe_close(self, key, turn, driver);
    }
}
