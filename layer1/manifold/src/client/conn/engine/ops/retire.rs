//! Retirement transition for connector-owned connections.
use std::pin;

use dope_core::driver::{result, retained, route::Identity, schedule};
use dope_net::link::{event, pool, slot::transmit};
use o3::base::cell::region;
use pool::{
    change::{
        close,
        drain::{Policy, release},
    },
    recv::input,
};

use crate::{
    app::env,
    client::{
        self, app, attempt,
        close::{Directive, Reason},
        conn::{
            self, claim, drain,
            engine::{self, liveness, ops::fail},
            state,
        },
        deadline, side,
        side::mode::Mode,
    },
    timing,
};

/// Retirement retains separate application and attempt-controller loans.
type Participants<'app, 'ctrl, A, S> = (&'app mut A, &'ctrl mut S);

pub(super) trait Output {
    type Value;

    fn retired(rule: Directive) -> (Reason, Self::Value);
}

pub(super) enum Outcome<'d, ID: Identity, V> {
    Retired(V),
    Pending,
    Missing(drain::Absent<'d, ID>),
    Obsolete(drain::Obsolete<'d, ID>),
}

pub(in crate::client) enum OwnedOutcome<'d, ID: Identity, V> {
    Retired(Retired<'d, ID, V>),
    Pending,
    Missing(Missing<'d, ID>),
    Obsolete(Obsolete<'d, ID>),
}

pub(in crate::client) struct Missing<'d, ID: Identity> {
    owner: attempt::Id<'d, ID>,
    proof: drain::Absent<'d, ID>,
}

impl<'d, ID: Identity> Missing<'d, ID> {
    pub(super) fn resolve<T>(self, resolve: impl FnOnce(attempt::Id<'d, ID>) -> T) -> T {
        self.proof.resolve();
        resolve(self.owner)
    }
}

pub(in crate::client) struct Obsolete<'d, ID: Identity> {
    owner: attempt::Id<'d, ID>,
    proof: drain::Obsolete<'d, ID>,
}

impl<'d, ID: Identity> Obsolete<'d, ID> {
    pub(super) fn resolve<T>(self, resolve: impl FnOnce(attempt::Id<'d, ID>) -> T) -> T {
        self.proof.resolve();
        resolve(self.owner)
    }
}

pub(in crate::client) struct Retired<'d, ID: Identity, V> {
    owner: attempt::Id<'d, ID>,
    value: V,
}

impl<'d, ID: Identity, V> Retired<'d, ID, V> {
    pub(super) fn resolve<T>(self, resolve: impl FnOnce(attempt::Id<'d, ID>, V) -> T) -> T {
        resolve(self.owner, self.value)
    }
}

impl<'d, ID: Identity, V> drain::BindOwner<'d, ID> for Outcome<'d, ID, V> {
    type Bound = OwnedOutcome<'d, ID, V>;

    fn bind(self, owner: attempt::Id<'d, ID>) -> Self::Bound {
        match self {
            Self::Retired(value) => OwnedOutcome::Retired(Retired { owner, value }),
            Self::Pending => OwnedOutcome::Pending,
            Self::Missing(proof) => OwnedOutcome::Missing(Missing { owner, proof }),
            Self::Obsolete(proof) => OwnedOutcome::Obsolete(Obsolete { owner, proof }),
        }
    }
}

impl Output for () {
    type Value = ();

    fn retired(rule: Directive) -> (Reason, Self::Value) {
        (rule.reason(), ())
    }
}

impl Output for Directive {
    type Value = Self;

    fn retired(rule: Directive) -> (Reason, Self::Value) {
        (rule.reason(), rule)
    }
}

pub(super) trait RetirePhase<'w, 'd: 'w, ID: Identity, A, S, E>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn fail(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn drain_close<O: Output>(
        self: pin::Pin<&mut Self>,
        drain: drain::Drain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome<'d, ID, O::Value>;
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> RetirePhase<'w, 'd, ID, A, S, E>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID>,
    A::Failure: fail::Fail<'d, ID, A, S, E::Transport>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn fail(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let target = self.as_ref().backoff();
        let close = {
            let this = self.as_mut().project();
            let Some(mut slot) = this.links.get(key, target) else {
                return;
            };
            let close = match claim::CutClaim::request(
                &mut slot,
                Directive::cut_retry(Reason::Transport),
            ) {
                claim::CutClaim::Ready(close) => close,
                claim::CutClaim::Owned => return,
            };
            slot.clear();
            close
        };
        let drain = close.into_drain();
        match self.drain_close::<()>(drain, turn, driver) {
            Outcome::Retired(()) | Outcome::Pending => {}
            Outcome::Missing(proof) => proof.resolve(),
            Outcome::Obsolete(proof) => proof.resolve(),
        }
    }

    fn drain_close<O: Output>(
        self: pin::Pin<&mut Self>,
        drain: drain::Drain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome<'d, ID, O::Value> {
        let target = self.as_ref().backoff();
        let this = self.project();
        let (pool, liveness) = this.links.parts();
        let key = drain.key();
        if let Some(egress) = pool.egress_mut(key) {
            egress.queue.cancel_wait(driver.region_token());
        }
        let mut ctx = (this.app, this.controller);
        <<A::Input as input::Mode<A::Wire>>::Ingress as Policy<A::Wire>>::close(
            pool,
            key,
            turn.reborrow().maint(),
            driver,
            close::Handler {
                claim: drain,
                context: &mut ctx,
                owner: |scope: close::Owner<
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    'w,
                    'd,
                    ID,
                    _,
                    engine::Record<'d, ID, A>,
                    drain::Drain<'d, ID>,
                    Participants<'_, '_, A, S>,
                >| {
                    use close::{Decision, Phase};

                    let ((_, ctrl), drain, phase, flights, slot, driver) = scope;
                    let wire = match drain.authorize(slot) {
                        drain::Access::Current(wire) => wire,
                        drain::Access::Stale => return Decision::Stale,
                    };
                    match phase {
                        Phase::Prepare => {
                            if !slot.state.is_attached() {
                                return Decision::Ready;
                            }
                            if matches!(wire, claim::Transport::Preserved)
                                && !slot.terminal.is_done()
                            {
                                // SAFETY: this authorized native slot belongs to the pool paired
                                // with the retained liveness book for the entire close callback.
                                let source =
                                    unsafe { engine::raw::reunite(&mut *liveness, slot, key) };
                                let slot = liveness::Change::new(source, target).ensure_after(
                                    deadline::Fixed::Send,
                                    driver.driver_ref().clock().now(),
                                    <E::Timing as timing::Policy>::SEND_DEADLINE,
                                );
                                let finish = slot.sending().seal(flights, driver);
                                match finish {
                                    transmit::Finalize::Armed(wait) => {
                                        return Decision::External(wait);
                                    }
                                    transmit::Finalize::Rejected(failure) => {
                                        match failure.into_error().cause() {
                                            result::SubmitCause::Full => {
                                                return Decision::runnable(slot);
                                            }
                                            result::SubmitCause::IdentityExhausted => {
                                                slot.state.fail(conn::Transmit::Rejected(failure));
                                                let error = ctrl.rejected(
                                                    slot.state.owner.attempt(),
                                                    failure.into_error(),
                                                );
                                                slot.state
                                                    .closing
                                                    .apply(Directive::from_submit(error));
                                                slot.sending().discard();
                                                return Decision::Ready;
                                            }
                                        }
                                    }
                                    transmit::Finalize::Settled | transmit::Finalize::Abandoned => {
                                    }
                                }
                            }
                            Decision::Ready
                        }
                        Phase::Release => {
                            // SAFETY: release retains the same authorized slot and paired book.
                            let source = unsafe { engine::raw::reunite(&mut *liveness, slot, key) };
                            liveness::Change::new(source, target).clear();
                            Decision::Ready
                        }
                    }
                },
                commit: |scope: close::Commit<
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    'd,
                    drain::Drain<'d, ID>,
                    Participants<'_, '_, A, S>,
                    engine::Record<'d, ID, A>,
                >| {
                    let ((app, _), drain, state, _, driver) = scope;
                    state.retire(drain, |barrier| app.quiesce(barrier, driver))
                },
                failed: |scope: close::Failed<
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    'w,
                    'd,
                    ID,
                    _,
                    engine::Record<'d, ID, A>,
                    _,
                    { client::IOV_CAP },
                    _,
                    drain::Drain<'d, ID>,
                    Participants<'_, '_, A, S>,
                >| {
                    let ((app, ctrl), _, io, driver) = scope;
                    let close::Failure::Receive(error) = io.failure();
                    let (slot, queue) = io.into_parts();
                    match state::Io::new(slot) {
                        state::Io::Attached(conn) => {
                            app.io_fail::<side::Primary<'d, ID>>(
                                conn,
                                queue.reborrow(),
                                error,
                                driver,
                            );
                        }
                        state::Io::Opening(owner) => {
                            <A::Failure as fail::Fail<'d, ID, A, S, E::Transport>>::fail(
                                ctrl,
                                app,
                                owner.attempt(),
                                event::ConnectError::Receive(error),
                                driver,
                            );
                        }
                    }
                    slot.state
                        .closing
                        .apply(Directive::reconnect(Reason::Transport));
                },
            },
            release::Borrowed::new(
                |ctx: &mut Participants<'_, '_, A, S>,
                 payload: A::Send,
                 token: &mut region::Token<region::Identity<'d, 1>>| {
                    ctx.0.cancel(payload, token)
                },
            ),
            |pool, ctx, driver, outcome| match outcome {
                close::Outcome::Retired(retired) => {
                    let output = retired.resolve(O::retired, |retired, state| {
                        ctx.0.drained(retired, state, driver);
                    });
                    Outcome::Retired(output)
                }
                close::Outcome::Pending(drain, deferred) => {
                    match deferred {
                        close::Deferred::Terminal
                        | close::Deferred::Setup
                        | close::Deferred::Send
                        | close::Deferred::Recv
                        | close::Deferred::Ingress
                        | close::Deferred::Egress
                        | close::Deferred::Scheduled => {}
                        close::Deferred::Owner(wait) => {
                            if !pool
                                .inspect()
                                .get(key)
                                .is_some_and(|slot| slot.send_status().is_waiting())
                            {
                                wait.reschedule();
                            }
                        }
                        close::Deferred::Retire(next) => next.reschedule(),
                    }
                    match drain.release(pool) {
                        drain::Recovery::Restored | drain::Recovery::Rescheduled => {
                            Outcome::Pending
                        }
                        drain::Recovery::Obsolete(proof) => Outcome::Obsolete(proof),
                    }
                }
                close::Outcome::Stale(drain) => Outcome::Obsolete(drain.obsolete()),
                close::Outcome::Missing(drain) => Outcome::Missing(drain.absent()),
            },
        )
    }
}
