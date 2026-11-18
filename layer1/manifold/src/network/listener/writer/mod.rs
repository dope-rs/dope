use std::pin;

use data::{compose, exec};
use dope_core::driver::{self, result::SubmitCause, route::Identity, schedule, schedule::clock};
use dope_net::link::{
    egress::data,
    pool,
    pool::{
        change::{close, drain},
        recv::input,
    },
    slot::transmit,
};
pub(in crate::network::listener) use egress::Egress;
use handler::retire;
use o3::{base::time::Positive, depot, depot::heap};
use retire::Policy as _;

use crate::{
    network::listener::{self, conn, handler, runtime::accept, writer, writer::deadline::Slot},
    timing,
};

pub(in crate::network::listener) mod deadline;
mod egress;
pub(in crate::network::listener) mod flow;
pub(in crate::network::listener) mod memory;
pub(in crate::network::listener) mod phase;
pub(in crate::network::listener) mod state;

/// Header loans and application egress share the same writer residency.
pub(in crate::network::listener) type Payload<'p, 's, 'd, ID, B> =
    compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>;
/// Payload mode selects the fixed writer reservation contract.
pub(in crate::network::listener) type Reserve<'d, B> =
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
pub(in crate::network::listener) type Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A> = pool::Entry<
    'a,
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
pub(in crate::network::listener) type Closing<'app, 'accept, 'deadline, 'p, 'd, ID, A> = (
    pin::Pin<&'app mut A>,
    pin::Pin<&'accept mut accept::Accept<'p, 'd, ID>>,
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
    pub(in crate::network::listener) pool: Connections<'n, 'p, 's, 'w, 'd, ID, T, A>,
    pub(in crate::network::listener) writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
    pub(in crate::network::listener) deadlines: deadline::Schedule<'d, ID>,
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

    pub(in crate::network::listener) fn egress_mut(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> Option<writer::Egress<'n, '_, 'p, 's, 'w, 'd, ID, A>> {
        let Self {
            pool,
            writes,
            deadlines: book,
        } = self;
        Some(writer::Egress::new(pool.egress_mut(key)?, *writes, book))
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
    ) -> Option<deadline::Event<'d, ID>> {
        self.deadlines.pop(now)
    }
}

pub(in crate::network::listener) fn close<
    'a,
    'n,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    A,
    T: dope_net::Transport,
>(
    mut entry: Result<
        Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A>,
        pool::Missing<'a, 'n, 'w, 'd, ID, A::Wire>,
    >,
    writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
    ctx: &mut Closing<'_, '_, '_, 'p, 'd, ID, A>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A>>
where
    A: handler::Application<'d, ID>,
    <A::Input as input::Mode<A::Wire>>::Ingress: drain::Policy<A::Wire>,
{
    let key = match &entry {
        Ok(entry) => entry.get().key(),
        Err(missing) => missing.key(),
    };
    if let Ok(admitted) = &mut entry {
        let slot = admitted.get();
        slot.io().close();
        if slot.terminal.is_pending() {
            if slot.terminal.has_send() {
                admitted.pending().wake();
            }
            return entry.ok();
        }
        if let Some(egress) = admitted.egress_mut() {
            let lane = pool::raw::Physical::bind(key);
            memory::Retention::new(writes, lane).cancel_wait(egress.connection.io().wake_target());
            egress.queue.cancel_wait(driver.region_token());
        }
    };
    let outcome = <<A::Input as input::Mode<A::Wire>>::Ingress as drain::Policy<A::Wire>>::close(
        entry,
        turn.reborrow().maint(),
        driver,
        close::Handler {
            claim: (),
            context: ctx,
            owner: |scope: close::Owner<
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
                Closing<'_, '_, '_, 'p, 'd, ID, A>,
            >| {
                use close::{Decision, Phase};

                let ((app, accept, due), claim, phase, slots, conn, driver) = scope;
                let key = conn.key();
                // SAFETY: the retirement scope lends the initialized slot for
                // this exact key together with the schedule covering it.
                let mut slot =
                    Slot::from(unsafe { depot::raw::Proof::new(((&mut *conn, &mut **due), key)) });
                let () = *claim;

                match phase {
                    Phase::Prepare => {
                        let live = slot.is_connected();
                        if live && !slot.is_discarded() && !slot.terminal.is_done() {
                            slot.closing(driver.driver_ref().clock().now());
                            match slot.sending().seal(slots, driver) {
                                transmit::Finalize::Armed(wait) => Decision::External(wait),
                                transmit::Finalize::Rejected(failure) => {
                                    if matches!(failure.into_error().cause(), SubmitCause::Full) {
                                        return Decision::runnable(&slot);
                                    }
                                    slot.state.send.release(
                                        Some(conn::Transmit::Rejected(failure)),
                                        |egress| {
                                            writer::release_egress(
                                                app.as_mut(),
                                                key,
                                                egress,
                                                handler::Disposition::Closed,
                                            );
                                        },
                                    );
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
                                Ctx::sealed(slot.sealed(), turn.reborrow().app()),
                            );
                        }
                        if let Some(admit) = conn.state.admission.take() {
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
                'd,
                (),
                Closing<'_, '_, '_, 'p, 'd, ID, A>,
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
                Closing<'_, '_, '_, 'p, 'd, ID, A>,
            >| {
                let (_, claim, io, _) = scope;
                let error = io.failure();
                let () = *claim;
                io.connection().state.fail(conn::Failure::Receive(error));
            },
        },
        |(app, _accept, _due): &mut (pin::Pin<&mut A>, _, _), data, _| {
            writer::release_payload(app.as_mut(), key, data, handler::Disposition::Closed);
        },
    );
    let (app, _accept, _due) = ctx;
    match outcome {
        close::Outcome::Retired((egress, retired)) => {
            if let Some(egress) = egress {
                writer::release_egress(app.as_mut(), key, egress, handler::Disposition::Closed);
            }
            if let Some(retired) = retired
                && let Some(policy) = <A::Retirement as retire::Policy<'d, A, ID>>::select()
            {
                policy.retire(app.as_mut(), retired);
            }
            None
        }
        close::Outcome::Pending(entry, ()) | close::Outcome::Stale(entry, ()) => Some(entry),
        close::Outcome::Missing(()) => None,
    }
}
