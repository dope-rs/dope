use std::pin;

use app::resume;
use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::link::{
    egress,
    pool::{self, pending},
    slot::{transmit::outcome::Retention, types},
};
use effect::Effect as _;
use engine::ops::close::{self, ClosePhase as _};

use crate::{
    app::{
        env,
        receive::{self, ingress},
    },
    client::{
        app, attempt,
        close::Reason,
        conn::{
            self, State,
            engine::{
                self, liveness,
                transfer::{effect, send},
            },
            flow, state,
        },
        side::mode::Mode,
    },
};

pub(in crate::client) trait Flush<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>:
    Sized
where
    A: app::Receive<'d, ID, Continuation = Self> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn admit<'machine, 'turn, 'driver, 'context, 'owner>(
        engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> Option<(types::Demand, Option<Reason>)>
    where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>;

    /// Flushes available work while peer EOF retains the application's drain fence.
    fn flush<'machine, 'turn, 'driver, 'context, 'owner>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        enum Demand {
            Egress,
            Terminal,
            Close(Reason),
            Idle,
            Gone,
        }

        let round =
            match pending::Pending::of(engine.as_ref().project_ref().links.pool()).try_round() {
                Ok(round) => round,
                Err(pending::Busy) => return,
            };
        round.run(turn.reborrow().maint(), |key| {
            let drain = {
                let this = engine.as_mut().project();
                this.links
                    .pool_mut()
                    .get_mut(key)
                    .is_some_and(|slot| slot.state.work.take_drain() == state::Taken::Ready)
            };
            if drain {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::drain(
                    engine.as_mut(),
                    key,
                    turn.reborrow(),
                    driver,
                );
            }
            let Some((ctrl, reason)) = Self::admit(engine.as_mut(), key, turn.reborrow(), driver)
            else {
                return pending::Disposition::Gone;
            };
            let capacity = pending::Pending::of(engine.as_ref().project_ref().links.pool())
                .at(key)
                .is_some_and(pending::Handle::has_capacity);
            if ctrl == types::Demand::Discard {
                let reason = match reason {
                    Some(reason) => reason,
                    None => Reason::Transport,
                };
                engine
                    .as_mut()
                    .close_slot(key, reason, turn.reborrow(), driver);
            } else {
                let demand = {
                    let fields = engine.as_ref().project_ref();
                    match fields.links.pool().inspect().egress(key) {
                        None => Demand::Gone,
                        Some((slot, pending)) => {
                            let status = slot.send_status();
                            let cont = slot.state.work.has_next();
                            let plain = !status.is_inflight()
                                && (capacity
                                    || status.retention() == Retention::Held
                                    || (!status.is_waiting()
                                        && (matches!(pending, egress::Pending::Present)
                                            || status.is_closing())));
                            let empty = matches!(pending, egress::Pending::Absent)
                                && !status.is_inflight()
                                && status.retention() == Retention::Clear
                                && !status.is_closing()
                                && !status.is_waiting()
                                && !cont;
                            let retire = empty && slot.terminal.kind().is_none();
                            match slot.demand() {
                                types::Demand::Discard | types::Demand::Close if retire => slot
                                    .state
                                    .closing
                                    .reason()
                                    .map_or(Demand::Idle, Demand::Close),
                                types::Demand::Discard
                                | types::Demand::Barrier
                                | types::Demand::Close => Demand::Idle,
                                types::Demand::Terminal if cont => Demand::Idle,
                                types::Demand::Terminal => Demand::Terminal,
                                types::Demand::Continue if plain => Demand::Egress,
                                types::Demand::Continue if retire => {
                                    match slot.state.closing.reason() {
                                        Some(Reason::Remote) if slot.is_draining() => Demand::Idle,
                                        Some(reason) => Demand::Close(reason),
                                        None => Demand::Idle,
                                    }
                                }
                                types::Demand::Continue => Demand::Idle,
                            }
                        }
                    }
                };
                match demand {
                    Demand::Egress => <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                        engine.as_mut(),
                        key,
                        turn.reborrow(),
                        driver,
                    ),
                    Demand::Terminal => <A::Requests as send::Policy<'w, 'd, ID, A>>::finish(
                        engine.as_mut(),
                        key,
                        turn.reborrow(),
                        driver,
                    ),
                    Demand::Close(reason) => {
                        engine
                            .as_mut()
                            .close_slot(key, reason, turn.reborrow(), driver)
                    }
                    Demand::Idle => {
                        engine.as_mut().maybe_close(key, turn.reborrow(), driver);
                    }
                    Demand::Gone => {}
                }
            }
            let this = engine.as_ref().project_ref();
            if this.links.pool().inspect().get(key).is_some() {
                pending::Disposition::Idle
            } else {
                pending::Disposition::Gone
            }
        });
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Flush<'p, 'w, 'd, ID, A, S, E, X>
    for resume::Complete
where
    A: app::Receive<'d, ID, Continuation = resume::Complete> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn admit<'machine, 'turn, 'driver, 'context, 'owner>(
        engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        _: schedule::Turn<'turn, 'd>,
        _: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> Option<(types::Demand, Option<Reason>)>
    where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        engine
            .as_ref()
            .project_ref()
            .links
            .pool()
            .inspect()
            .get(key)
            .map(|slot| (slot.demand(), slot.state.closing.reason()))
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Flush<'p, 'w, 'd, ID, A, S, E, X>
    for resume::Retained
where
    A: app::Resumable<'d, ID, Continuation = resume::Retained> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn admit<'machine, 'turn, 'driver, 'context, 'owner>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> Option<(types::Demand, Option<Reason>)>
    where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let state = engine
            .as_ref()
            .project_ref()
            .links
            .pool()
            .inspect()
            .get(key)
            .map(|slot| {
                (
                    slot.demand(),
                    slot.state.closing.reason(),
                    slot.state.work.has_next(),
                )
            });
        let (control, reason, pending) = state?;
        if control == types::Demand::Discard || !pending {
            return Some((control, reason));
        }
        let Some(permit) = turn.reborrow().app().permit() else {
            let this = engine.as_mut().project();
            if let Some(handle) = pending::Pending::of(this.links.pool()).at(key) {
                handle.wake();
            }
            return Some((control, reason));
        };
        let callback = {
            let this = engine.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: _,
                queue: egress,
            }) = this.links.pool_mut().egress_mut(key)
            else {
                return Some((control, reason));
            };
            if slot.state.work.take_next() != state::Taken::Ready {
                return Some((control, reason));
            }
            match State::active(slot, |active| {
                app::Resumable::resume(
                    this.app,
                    permit,
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    driver,
                )
            }) {
                flow::Active::Ready(callback) => callback,
                flow::Active::Opening => {
                    slot.state.work.request_next();
                    return Some((control, reason));
                }
                flow::Active::Sealed => return Some((control, reason)),
            }
        };
        let (done, next) = callback.into_parts();
        let outcome = match done {
            app::Resumed::Complete(done) => {
                engine.as_mut().project().links.pool_mut().resume(key);
                done.apply(engine.as_mut(), key, next, turn.reborrow(), driver)
            }
            app::Resumed::Paused(pause) => {
                match pause.apply(engine.as_mut(), key, next, turn.reborrow(), driver) {
                    receive::Outcome::Continue => return Some((control, reason)),
                    outcome => outcome,
                }
            }
        };
        match outcome {
            receive::Outcome::Continue => {
                engine.as_mut().maybe_close(key, turn.reborrow(), driver);
            }
            receive::Outcome::Failed(error) => {
                engine
                    .as_mut()
                    .cut_close(key, error.into(), turn.reborrow(), driver);
            }
            receive::Outcome::CloseAfter => {
                engine
                    .as_mut()
                    .project()
                    .links
                    .pool_mut()
                    .recv()
                    .close_after(key);
                engine.as_mut().maybe_close(key, turn.reborrow(), driver);
            }
        }
        Some((control, reason))
    }
}
