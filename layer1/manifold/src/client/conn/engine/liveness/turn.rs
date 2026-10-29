use std::{pin, task};

use dope_core::driver::{
    retained,
    route::Identity,
    schedule,
    schedule::{
        clock,
        ready::{self, wake},
    },
};
use dope_net::link::{oneshot, pool};
use engine::ops::close::{self, ClosePhase as _};
use o3::mem::quota;

use crate::{
    app::env,
    client::{
        app, attempt,
        close::Reason,
        conn::{
            self,
            engine::{self, liveness},
            state,
        },
        deadline, side,
        side::{mode::Mode, registry},
    },
    timing,
};

enum Expiry<'d, ID: Identity> {
    Ignore,
    Close(pool::Key<'d, ID>, deadline::Kind),
}

pub(in crate::client::conn::engine) struct Turn<'a, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    machine: pin::Pin<&'a mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
}

impl<'p, 'a, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Turn<'a, 'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    pub(in crate::client::conn::engine) fn new(
        this: pin::Pin<&'a mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    ) -> Self {
        Self { machine: this }
    }

    fn poll_alarm(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> task::Poll<(clock::Moment<'d>, ready::Target<'d>)> {
        if !self
            .machine
            .as_ref()
            .project_ref()
            .links
            .liveness()
            .deadlines
            .is_armed()
        {
            return task::Poll::Pending;
        }
        let now = driver.driver_ref().clock().now();
        let target = self.machine.as_ref().backoff();
        let wake = wake::Waker::from(target);
        self.machine
            .as_mut()
            .project()
            .links
            .liveness_mut()
            .deadlines
            .poll(now, wake)
            .map(|()| (now, target))
    }

    fn expire_book(
        &mut self,
        now: clock::Moment<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            let step = turn.reborrow().maint().run(|| {
                let this = self.machine.as_mut().project();
                let (pool, liveness) = this.links.parts();
                let item = liveness.deadlines.book.pop_expired(now)?;
                let key = item.key();
                let view = pool.inspect();
                let done = if let Some(slot) = view.get(key) {
                    item.classify(
                        |item| {
                            let watch = state::ActiveRef::new(slot).map(|active| {
                                <A::Continuation as liveness::Inspection<'d, ID, A>>::inspect(
                                    &*this.app,
                                    conn::Ref::new(active),
                                    <E::Timing as timing::Policy>::IDLE_WINDOW,
                                    &mut *driver,
                                )
                            });
                            match watch {
                                None | Some(liveness::Observation::Quiescent) => {
                                    item.cancel_recv();
                                    Expiry::Ignore
                                }
                                Some(liveness::Observation::Awaiting { window, seen }) => {
                                    use deadline::book::Rearm;

                                    match item.rearm_recv(now, Some(seen), window) {
                                        Rearm::Future => Expiry::Ignore,
                                        Rearm::Expired => {
                                            Expiry::Close(key, deadline::Kind::Inbound)
                                        }
                                    }
                                }
                            }
                        },
                        |item, kind| {
                            item.clear();
                            Expiry::Close(key, kind)
                        },
                    )
                } else {
                    item.clear();
                    Expiry::Ignore
                };
                Some(done)
            });
            let done = match step {
                quota::Admission::Item(done) => done,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            match done {
                Expiry::Ignore => {}
                Expiry::Close(key, kind) => self.machine.as_mut().cut_close(
                    key,
                    Reason::Timeout(kind),
                    turn.reborrow(),
                    driver,
                ),
            }
        }
    }
}

impl<'p, 'a, 'w, 'd: 'w, ID: Identity + 'd, A, S, E>
    Turn<'a, 'p, 'w, 'd, ID, A, S, E, side::Disabled>
where
    A: app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    engine::Machine<'p, 'w, 'd, ID, A, S, E>: close::ClosePhase<'d, ID, A, S, E>,
{
    pub(in crate::client::conn::engine) fn poll(
        mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let task::Poll::Ready((now, target)) = self.poll_alarm(driver) else {
            return;
        };
        self.expire_book(now, turn, driver);
        self.machine
            .as_mut()
            .project()
            .links
            .liveness_mut()
            .rearm(target);
    }
}

impl<'p, 'a, 'r, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, I>
    Turn<'a, 'p, 'w, 'd, ID, A, S, E, side::Enabled<&'r registry::Registry<'d, I, ID>>>
where
    A: app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, side::Enabled<&'r registry::Registry<'d, I, ID>>>:
        close::ClosePhase<'d, ID, A, S, E>,
{
    pub(in crate::client::conn::engine) fn poll(
        mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let task::Poll::Ready((now, target)) = self.poll_alarm(driver) else {
            return;
        };
        loop {
            let step = turn.reborrow().maint().run(|| {
                Some(
                    self.machine
                        .as_mut()
                        .project()
                        .links
                        .liveness_mut()
                        .expire_one(now, driver),
                )
            });
            match step {
                quota::Admission::Item(oneshot::Progress::Complete {
                    authority: owner,
                    result: done,
                }) => {
                    let done = match done {
                        Ok(()) => Ok(()),
                        Err(error) => Err(side::Error::Delivery(error)),
                    };
                    self.machine
                        .as_mut()
                        .project()
                        .side
                        .registry
                        .complete(owner, done);
                }
                quota::Admission::Item(oneshot::Progress::Idle) => break,
                quota::Admission::Item(oneshot::Progress::Pending) => {}
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            }
        }
        self.expire_book(now, turn, driver);
        self.machine
            .as_mut()
            .project()
            .links
            .liveness_mut()
            .rearm(target);
    }
}
