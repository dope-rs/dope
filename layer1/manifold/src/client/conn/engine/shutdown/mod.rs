use o3::{base::Call as _, mem::quota};

pub(in crate::client) mod state;

use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::link::oneshot;
use engine::ops::close::{self, ClosePhase as _};

use crate::{
    app::env,
    client::{
        app, attempt,
        close::{LocalOrigin, Reason},
        conn::engine::{self},
        side::{self, registry},
    },
};

fn settle<'d, I: registry::Intent<'d, ID>, ID: Identity + 'd>(
    registry: &registry::Registry<'d, I, ID>,
    done: oneshot::Progress<side::Ticket<'d, ID>>,
) {
    if let oneshot::Progress::Complete {
        authority: owner, ..
    } = done
    {
        registry.complete(owner, Err(side::Error::RuntimeShutdown));
    }
}

pub(crate) trait Drain<'d> {
    fn begin(self: pin::Pin<&mut Self>);
    fn drain(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E> Drain<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, side::Disabled>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    Self: close::ClosePhase<'d, ID, A, S, E>,
{
    fn begin(mut self: pin::Pin<&mut Self>) {
        let state::Start::Started = self.as_mut().project().shutdown.begin() else {
            return;
        };
        let mut this = self.as_mut().project();
        this.drive.as_mut().stop();
        if let Some(call) = A::shutdown() {
            call.call(this.app);
        }
    }

    fn drain(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            let step = turn.reborrow().maint().run(|| {
                let this = self.as_mut().project();
                let capacity = this.links.pool().inspect().capacity();
                let plan = this.shutdown.plan_lane(capacity)?;
                Some(plan.complete())
            });
            let lane = match step {
                quota::Admission::Item(state::Primary::Lane(lane)) => lane,
                quota::Admission::Item(state::Primary::Done)
                | quota::Admission::Empty
                | quota::Admission::Exhausted => return,
            };
            if let Some(key) = self
                .as_ref()
                .project_ref()
                .links
                .pool()
                .inspect()
                .key_at(lane)
            {
                self.as_mut().close_slot(
                    key,
                    Reason::Local(LocalOrigin::RuntimeShutdown),
                    turn.reborrow(),
                    driver,
                );
            }
        }
    }
}

impl<'p, 'r, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, I> Drain<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, side::Enabled<&'r registry::Registry<'d, I, ID>>>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
    Self: close::ClosePhase<'d, ID, A, S, E>,
{
    fn begin(mut self: pin::Pin<&mut Self>) {
        let state::Start::Started = self.as_mut().project().shutdown.begin() else {
            return;
        };
        let target = self.as_ref().backoff();
        let mut this = self.as_mut().project();
        this.drive.as_mut().stop();
        this.links.liveness_mut().resume_retry(target);
        this.side.registry.close_intake();
        if let Some(call) = A::shutdown() {
            call.call(this.app);
        }
    }

    fn drain(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            let step = turn.reborrow().maint().run(|| {
                let this = self.as_mut().project();
                let queue = this.side.registry.pending().intake();
                let capacity = this.links.pool().inspect().capacity();
                let plan = this.shutdown.plan(queue, capacity)?;
                match plan {
                    state::Plan::Intake(cursor) => match this.side.registry.reject() {
                        registry::Rejection::Settled => {
                            cursor.intake();
                            Some(state::Step::Intake)
                        }
                        registry::Rejection::Empty => None,
                    },
                    state::Plan::Lane(cursor, lane, next) => {
                        cursor.advance(next);
                        Some(state::Step::Lane(lane))
                    }
                    state::Plan::Cancel(cursor) => {
                        let Some(done) = this.links.liveness_mut().cancel_next(cursor, driver)
                        else {
                            *this.shutdown = state::State::Done;
                            return Some(state::Step::Done);
                        };
                        settle(this.side.registry, done);
                        Some(state::Step::Cancel)
                    }
                }
            });
            let lane = match step {
                quota::Admission::Item(state::Step::Intake | state::Step::Cancel) => continue,
                quota::Admission::Item(state::Step::Lane(lane)) => lane,
                quota::Admission::Item(state::Step::Done)
                | quota::Admission::Empty
                | quota::Admission::Exhausted => return,
            };
            if let Some(key) = self
                .as_ref()
                .project_ref()
                .links
                .pool()
                .inspect()
                .key_at(lane)
            {
                self.as_mut().close_slot(
                    key,
                    Reason::Local(LocalOrigin::RuntimeShutdown),
                    turn.reborrow(),
                    driver,
                );
            }
            let this = self.as_mut().project();
            let mut cursor = lane.index().get();
            if let Some(done) = this.links.liveness_mut().cancel_next(&mut cursor, driver) {
                settle(this.side.registry, done);
            }
        }
    }
}
