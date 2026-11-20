use std::{mem::MaybeUninit, pin};

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::{egress, event, pool, pool::pending::Handle, slot::transmit},
    wire::send::reclaim,
};
use o3::{depot, mem::slot};

use crate::network::listener::{
    self, conn, handler,
    writer::{
        self, state,
        deadline::{self, Slot},
    },
};

use super::{Wake, flow};

pub(super) type Source<'a, 'n, 'p, 's, 'w, 'd, ID, A, T> = transmit::Sender<
    'a,
    'n,
    'w,
    'd,
    ID,
    T,
    <A as handler::Application<'d, ID>>::Wire,
    writer::Record<'p, 's, 'd, ID, A>,
    <A as handler::Application<'d, ID>>::Input,
    writer::Payload<'p, 's, 'd, ID, <A as handler::Application<'d, ID>>::Egress>,
    { listener::IOV_CAP },
    writer::Reserve<'d, <A as handler::Application<'d, ID>>::Egress>,
>;

fn view<'a, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &'a mut Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    schedule: &'a mut deadline::Schedule<'d, ID>,
) -> (
    &'a crate::Flights<'d, ID>,
    Slot<'a, 'p, 's, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
    Handle<'a, 'd, ID>,
    egress::Queue<
        'a,
        'n,
        'd,
        { listener::IOV_CAP },
        writer::Payload<'p, 's, 'd, ID, A::Egress>,
        writer::Reserve<'d, A::Egress>,
    >,
)
where
    A: handler::Application<'d, ID>,
{
    let pool::EgressMut {
        flights,
        connection: conn,
        pending,
        queue,
    } = source.view();
    let key = conn.key();
    (
        flights,
        // SAFETY: the admitted sender and schedule are borrowed from the same writer.
        Slot::from(unsafe { depot::raw::Proof::new(((conn, schedule), key)) }),
        pending,
        queue,
    )
}

pub(super) fn commit_end<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &mut Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    schedule: &mut deadline::Schedule<'d, ID>,
    app: pin::Pin<&mut A>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Application<'d, ID>,
{
    let (slots, mut slot, pending, _) = view::<ID, A, T>(source, schedule);
    let key = slot.key();
    let finish = slot.sending().finish(slots, driver);
    pending.wait_send(slot.send_status().is_waiting());
    match finish {
        transmit::Finalize::Armed(()) => {
            slot.ensure_send(driver.driver_ref().clock().now());
        }
        transmit::Finalize::Rejected(failure) => {
            let mut place = MaybeUninit::uninit();
            let closed = slot.state.send.take(
                Some(conn::Transmit::Rejected(failure)),
                slot::Vacant::new(&mut place),
            );
            slot.cancel_send();
            if let Some(body) = closed {
                app.released(conn::Id::from_key(key), body.into_inner(), handler::Disposition::Closed);
            }
        }
        transmit::Finalize::Settled | transmit::Finalize::Abandoned => slot.cancel_send(),
    }
}

pub(super) fn advance<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &mut Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    schedule: &mut deadline::Schedule<'d, ID>,
    writes: &'p writer::memory::Arena<'s, 'd, ID, A::Egress>,
    mut app: pin::Pin<&mut A>,
    wake: Wake,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Runtime<'w, 'd, ID>,
{
    let mut projection = view::<ID, A, T>(source, schedule);
    let (slots, slot, handle, egress) = &mut projection;
    let key = slot.key();
    let result = 'send: {
        let flow = match wake {
            Wake::Maintenance(flow) => {
                slot.ensure_send(driver.driver_ref().clock().now());
                flow
            }
            Wake::Io | Wake::Progress => {
                if matches!(wake, Wake::Progress) {
                    slot.progress(driver.driver_ref().clock().now());
                }
                if slot.is_closing() {
                    break 'send Ok(flow::Flow::Clear);
                }
                slot.flow(egress.pending()).retry()
            }
        };
        match flow {
            flow::Flow::Inflight => Ok(flow::Flow::Inflight),
            flow::Flow::Done => Ok(flow::Flow::Done),
            flow::Flow::Plain => {
                slot.advance_send(slots, *handle, driver);
                let pending = egress.pending();
                Ok(slot.flow(pending))
            }
            flow::Flow::Stalled | flow::Flow::Held | flow::Flow::Clear => {
                use transmit::outcome;

                let mode = slot.state.send.mode();
                let progress = match &mode {
                    state::Mode::Queued => slot.sending().flush_queue(slots, driver),
                    state::Mode::Direct => slot.sending().flush_direct(slots, driver),
                };
                handle.wait_send(slot.send_status().is_waiting());
                match progress {
                    outcome::Progress::Submitted | outcome::Progress::Idle => {
                        let pending = egress.pending();
                        Ok(slot.flow(pending))
                    }
                    outcome::Progress::Rejected(error) => {
                        slot.sending().discard();
                        let failure = match mode {
                            state::Mode::Queued => event::SendFailure::Queued(error),
                            state::Mode::Direct => event::SendFailure::Direct(error),
                        };
                        Err(conn::Transmit::Rejected(failure))
                    }
                }
            }
            flow::Flow::Deferred => {
                drop(projection);
                let done = source.submit(driver, |body| {
                    writer::release_payload(app.as_mut(), key, body, handler::Disposition::Sent);
                });
                projection = view::<ID, A, T>(source, schedule);
                let slot = &projection.1;
                match done {
                    transmit::Attempt::Runnable(
                        reclaim::Advance::Idle | reclaim::Advance::Ready,
                    ) => Ok(flow::Flow::Deferred),
                    transmit::Attempt::Waiting => {
                        if slot.send_status().is_inflight() {
                            Ok(flow::Flow::Inflight)
                        } else {
                            Ok(flow::Flow::Clear)
                        }
                    }
                    transmit::Attempt::Quiescent(
                        reclaim::Advance::Idle | reclaim::Advance::Ready,
                    ) => Ok(flow::Flow::Clear),
                    transmit::Attempt::Invalid(error) => Err(conn::Transmit::Invalid(error)),
                    transmit::Attempt::Rejected(
                        failure,
                        reclaim::Advance::Idle | reclaim::Advance::Ready,
                    ) => Err(conn::Transmit::Rejected(failure)),
                }
            }
        }
    };
    let (_, mut slot, handle, egress) = projection;
    let result = match result {
        Ok(result) => result,
        Err(failure) => {
            slot.state.send.release(app.as_mut(), conn::Id::from_key(key), Some(failure), handler::Disposition::Closed);
            flow::Flow::Clear
        }
    };
    if !matches!(wake, Wake::Maintenance(_))
        && (matches!(result, flow::Flow::Inflight)
            || matches!(egress.pending(), egress::Pending::Present))
    {
        slot.ensure_send(driver.driver_ref().clock().now());
    }

    match result {
        flow::Flow::Done => {
            slot.cancel_send();
            slot.state.send.release(
                app.as_mut(), conn::Id::from_key(key), None, handler::Disposition::Sent,
            );
            super::send::notify(source, schedule, writes, app, turn, driver);
        }
        flow::Flow::Plain | flow::Flow::Deferred => handle.wake(),
        _ => {}
    }
}
