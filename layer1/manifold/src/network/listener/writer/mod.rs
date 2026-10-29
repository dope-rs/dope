use std::pin;

use data::{compose, exec};
use dope_core::{
    driver::{result::SubmitCause, retained, route::Identity, schedule, schedule::clock},
    io::event::net::send::Completion,
};
use dope_net::link::{
    egress::data,
    event::SendOutcome,
    pool,
    pool::{
        change::{close, drain},
        recv::input,
        send,
    },
    slot::transmit,
};
pub(in crate::network::listener) use egress::Egress;
use handler::retire;
use o3::{
    base::{cell::region, time::Positive},
    depot,
    depot::heap,
};
use retire::Policy as _;

use crate::{
    network::listener::{
        self, conn, handler,
        runtime::accept,
        writer,
        writer::{deadline::Slot, phase::Sender},
    },
    timing,
};

pub(in crate::network::listener) mod deadline;
mod egress;
pub(in crate::network::listener) mod flow;
pub(in crate::network::listener) mod memory;
pub(in crate::network::listener) mod phase;
pub(in crate::network::listener) mod state;

/// Header loans and application egress share the same writer residency.
type Payload<'p, 's, 'd, ID, B> = compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>;
/// Payload mode selects the fixed writer reservation contract.
type Reserve<'d, B> =
    <<B as data::raw::Payload<'d>>::Mode as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>;
/// The installed application fixes connection state and payload residency together.
type Record<'p, 's, 'd, ID, A> = conn::State<
    'p,
    's,
    'd,
    ID,
    <A as handler::Application<'d, ID>>::Conn,
    <A as handler::Application<'d, ID>>::Egress,
>;
/// The installed pool retains the application's exact input and reservation policies.
type Connections<'n, 'p, 's, 'w, 'd, ID, T, A> = pool::Pool<
    'n,
    'w,
    'd,
    ID,
    T,
    <A as handler::Application<'d, ID>>::Wire,
    Record<'p, 's, 'd, ID, A>,
    <A as handler::Application<'d, ID>>::Input,
    Payload<'p, 's, 'd, ID, <A as handler::Application<'d, ID>>::Egress>,
    { listener::IOV_CAP },
    Reserve<'d, <A as handler::Application<'d, ID>>::Egress>,
>;
/// Retirement borrows each participant independently throughout all three phases.
type Closing<'app, 'accept, 'deadline, 'd, ID, A> = (
    pin::Pin<&'app mut A>,
    pin::Pin<&'accept mut accept::Accept<'d, ID>>,
    &'deadline mut deadline::Schedule<'d, ID>,
);
pub(in crate::network::listener) fn release_payload<
    'd,
    ID: Identity,
    A: handler::Application<'d, ID>,
>(
    mut app: pin::Pin<&mut A>,
    key: pool::Key<'d, ID>,
    body: Payload<'_, '_, 'd, ID, A::Egress>,
    cause: handler::Disposition,
) {
    if let compose::Either::Right(egress) = body {
        release_egress(app.as_mut(), key, egress, cause);
    }
}

pub(in crate::network::listener) fn release_egress<
    'd,
    ID: Identity,
    A: handler::Application<'d, ID>,
>(
    app: pin::Pin<&mut A>,
    key: pool::Key<'d, ID>,
    egress: A::Egress,
    cause: handler::Disposition,
) {
    use handler::send::{Dispatch as _, Policy as _};

    if let Some(report) = A::Send::release() {
        report.dispatch(app, conn::Id::from_key(key), egress, cause);
    }
}

pub(in crate::network::listener) struct Owner<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, T, A>
where
    T: dope_net::Transport,
    A: handler::Application<'d, ID>,
{
    pool: Connections<'n, 'p, 's, 'w, 'd, ID, T, A>,
    writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
    deadlines: deadline::Schedule<'d, ID>,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, T, A> Owner<'n, 'p, 's, 'w, 'd, ID, T, A>
where
    T: dope_net::Transport,
    A: handler::Application<'d, ID>,
{
    pub(in crate::network::listener) fn try_new<P: timing::Policy>(
        pool: Connections<'n, 'p, 's, 'w, 'd, ID, T, A>,
        writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
        life: handler::Lifetime,
        times: handler::Timing,
    ) -> Result<Self, heap::Error> {
        let book =
            deadline::Schedule::try_new::<P>(pool.inspect().capacity().count(), life, times)?;
        Ok(Self {
            pool,
            writes,
            deadlines: book,
        })
    }

    pub(in crate::network::listener) const fn pool(
        &self,
    ) -> &Connections<'n, 'p, 's, 'w, 'd, ID, T, A> {
        &self.pool
    }

    pub(in crate::network::listener) const fn pool_mut(
        &mut self,
    ) -> &mut Connections<'n, 'p, 's, 'w, 'd, ID, T, A> {
        &mut self.pool
    }

    pub(in crate::network::listener) fn cancel_wait(
        &mut self,
        token: &mut region::Token<region::Identity<'d>>,
        key: pool::Key<'d, ID>,
    ) {
        let Self { pool, writes, .. } = self;
        let Some(egress) = pool.egress_mut(key) else {
            return;
        };
        let lane = pool::raw::Physical::bind(key);
        let target = egress.connection.io().wake_target();
        memory::Retention::new(*writes, lane).cancel_wait(target);
        egress.queue.cancel_wait(token);
    }

    pub(in crate::network::listener) fn egress_mut(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> Option<writer::Egress<'n, '_, 'p, 's, 'w, 'd, ID, A>> {
        let Self {
            pool,
            writes,
            deadlines: book,
        } = self;
        let pool::EgressMut {
            flights: slots,
            connection: conn,
            pending: wait,
            queue,
            ..
        } = pool.egress_mut(key)?;
        let lane = pool::raw::Physical::bind(key);
        Some(writer::Egress {
            flights: slots,
            // SAFETY: `egress_mut` lent the initialized connection recorded for
            // this exact key, and the deadline schedule spans every key of that
            // pool's canonical lane capacity.
            connection: Slot::from(unsafe { depot::raw::Proof::new(((conn, book), key)) }),
            pending: wait,
            queue: queue.reborrow(),
            retention: memory::Retention::new(*writes, lane),
        })
    }
    pub(in crate::network::listener) fn earliest(&self) -> Option<clock::Deadline<'d>> {
        self.deadlines.earliest()
    }
    pub(in crate::network::listener) fn drain(&self) -> Option<Positive> {
        self.deadlines.drain()
    }
    pub(in crate::network::listener) fn pop(
        &mut self,
        now: clock::Moment<'d>,
    ) -> Option<(pool::Key<'d, ID>, deadline::Kind)> {
        self.deadlines.pop(now)
    }

    fn sender(&mut self, key: pool::Key<'d, ID>) -> Option<Sender<'_, 'n, 'p, 's, 'w, 'd, ID, A>> {
        let source = self.pool.sender(key)?;
        // SAFETY: `sender` lent the initialized transmit record for this exact
        // key, and the retained schedule and write arena cover that same lane.
        Some(Sender::from(unsafe {
            depot::raw::Proof::new(((source, &mut self.deadlines, self.writes), key))
        }))
    }

    fn settle<O>(
        &mut self,
        done: Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        direct: impl for<'a> FnOnce(send::Direct<'a, Record<'p, 's, 'd, ID, A>>) -> O,
        free: impl FnMut(pool::Key<'d, ID>, Payload<'p, 's, 'd, ID, A::Egress>),
    ) -> SendOutcome<'d, ID, O>
    where
        <A::Input as input::Mode<A::Wire>>::Ingress: send::Finish<A::Wire>,
    {
        self.pool
            .settle(done)
            .settle(driver, direct, free, |conn, outcome| {
                if !matches!(outcome, SendOutcome::Drop) {
                    let key = conn.key();
                    // SAFETY: `settle` hands back the initialized connection it
                    // just retired for this exact key, and the deadline schedule
                    // spans that same lane.
                    Slot::from(unsafe {
                        depot::raw::Proof::new(((conn, &mut self.deadlines), key))
                    })
                    .cancel_send();
                }
            })
    }

    pub(in crate::network::listener) fn close(
        &mut self,
        app: pin::Pin<&mut A>,
        accept: pin::Pin<&mut accept::Accept<'d, ID>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        <A::Input as input::Mode<A::Wire>>::Ingress: drain::Policy<A::Wire>,
    {
        self.cancel_wait(driver.region_token(), key);
        let mut ctx = (app, accept, &mut self.deadlines);
        <<A::Input as input::Mode<A::Wire>>::Ingress as drain::Policy<A::Wire>>::close(
            &mut self.pool,
            key,
            turn.reborrow().maint(),
            driver,
            close::Handler {
                claim: (),
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
                    Record<'p, 's, 'd, ID, A>,
                    (),
                    Closing<'_, '_, '_, 'd, ID, A>,
                >| {
                    use close::{Decision, Phase};

                    let ((app, accept, due), claim, phase, slots, slot, driver) = scope;
                    let key = slot.key();
                    // SAFETY: the retirement scope lends the initialized slot for
                    // this exact key together with the schedule covering it.
                    let mut slot =
                        Slot::from(unsafe { depot::raw::Proof::new(((slot, &mut **due), key)) });
                    let () = *claim;

                    match phase {
                        Phase::Prepare => {
                            let live = slot.is_connected();
                            if live && !slot.is_discarded() && !slot.terminal.is_done() {
                                slot.closing(driver.driver_ref().clock().now());
                                match slot.sending().seal(slots, driver) {
                                    transmit::Finalize::Armed(wait) => Decision::External(wait),
                                    transmit::Finalize::Rejected(failure) => {
                                        if matches!(failure.into_error().cause(), SubmitCause::Full)
                                        {
                                            return Decision::runnable(&slot);
                                        }
                                        let egress =
                                            slot.state.send.fail(conn::Transmit::Rejected(failure));
                                        if let Some(egress) = egress {
                                            writer::release_egress(
                                                app.as_mut(),
                                                key,
                                                egress,
                                                handler::Disposition::Closed,
                                            );
                                        }
                                        Decision::Ready
                                    }
                                    transmit::Finalize::Settled | transmit::Finalize::Abandoned => {
                                        Decision::Ready
                                    }
                                }
                            } else {
                                Decision::Ready
                            }
                        }
                        Phase::Release => {
                            slot.clear();
                            if slot.is_connected() {
                                use conn::Ctx;

                                A::close(
                                    app.as_mut(),
                                    Ctx::sealed(&mut slot, turn.reborrow().app()),
                                );
                            }
                            if let Some(admit) = slot.state.admission.take() {
                                accept.as_mut().release(admit);
                            }
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
                    (),
                    Closing<'_, '_, '_, 'd, ID, A>,
                    Record<'p, 's, 'd, ID, A>,
                >| {
                    close::Approval::Ready(conn::Retired::take(key, scope.2, scope.3))
                },
                failed: |scope: close::Failed<
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    '_,
                    'n,
                    'w,
                    'd,
                    ID,
                    _,
                    Record<'p, 's, 'd, ID, A>,
                    _,
                    { listener::IOV_CAP },
                    _,
                    (),
                    Closing<'_, '_, '_, 'd, ID, A>,
                >| {
                    let (_, claim, io, _) = scope;
                    let cause = io.failure();
                    let () = *claim;
                    let close::Failure::Receive(error) = cause;
                    io.connection().state.fail(conn::Failure::Receive(error));
                },
            },
            |(app, _accept, _due): &mut (pin::Pin<&mut A>, _, _), data| {
                writer::release_payload(app.as_mut(), key, data, handler::Disposition::Closed);
            },
            |pool, (app, _accept, _due), _driver, outcome| match outcome {
                close::Outcome::Retired((egress, retired)) => {
                    if let Some(egress) = egress {
                        writer::release_egress(
                            app.as_mut(),
                            key,
                            egress,
                            handler::Disposition::Closed,
                        );
                    }
                    if let Some(retired) = retired
                        && let Some(policy) = <A::Retirement as retire::Policy<'d, A, ID>>::select()
                    {
                        policy.retire(app.as_mut(), retired);
                    }
                }
                close::Outcome::Pending((), deferred) => match deferred {
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
                },
                close::Outcome::Stale(()) | close::Outcome::Missing(()) => {}
            },
        );
    }
}
