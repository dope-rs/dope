use dope_core::{
    driver::{self, route::Identity},
    io::event::net::send::Completion as Native,
};
use o3::base::cell::region;
use send::{Store as _, reclaim};

use crate::{
    link::{
        Consumed,
        duplex::sending,
        egress::{self, data, reserve::{demand, error}},
        event::{DirectStatus, SendFailure, SendOutcome, SendSource},
        pool::{self, recv::input},
        slot::{transmit::{self, graceful, outcome}, types},
    },
    wire::{self, send, terminal},
};

#[must_use]
pub enum Rejected<B> {
    ConnectionUnavailable(egress::NonEmpty<B>),
    PendingUnavailable(egress::NonEmpty<B>),
    Egress(error::Capacity<B>),
}

#[must_use = "publish the observed send completion"]
pub enum Completion<P> {
    Direct(Direct<P>),
    Ready(P, SendOutcome),
}

/// Direct retirement authority after queue and acknowledgement clearance.
pub struct Direct<P> {
    owner: P,
    status: DirectStatus,
}

impl<P> Direct<P> {
    pub fn into_parts(self) -> (P, DirectStatus) {
        (self.owner, self.status)
    }
}

#[must_use = "publish completion before dispatching the admitted sender"]
pub struct Publication<
    'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, T: crate::Transport,
    W: wire::Spec + 'w + 'd, S, M: input::Mode<W>,
    B: data::raw::Payload<'d>, const IOV: usize, D: demand::Mode,
> {
    sender: transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    space: send::Availability,
}

impl<'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Publication<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    pub fn connection(&mut self) -> &mut types::Connection<'w, 'd, ID, W, S> {
        self.sender.entry.get_mut()
    }

    pub fn publish(
        mut self,
        before: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>),
    ) -> transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        {
            let (_, connection, pending, _) = self.sender.entry.reborrow().into_transmit();
            pending.wait_send(connection.send_status().is_waiting());
            before(connection);
        }
        <M::Ingress as input::Dispatch<W>>::release::<ID>(
            self.space, &mut self.sender.entry.root.runtime,
        );
        if self.space == send::Availability::Released {
            self.sender.entry.root.pending.release_send();
        }
        self.sender
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    pub fn settle<C>(
        &mut self,
        done: Native<'d>,
        driver: &mut driver::Context<'_, 'd>,
        context: &mut C,
        before: impl FnOnce(&mut C, &mut types::Connection<'w, 'd, ID, W, S>),
        mut release: impl FnMut(
            &mut C, pool::Key<'d, ID>, B, &mut region::Token<region::Identity<'d, 1>>,
        ),
        complete: impl FnOnce(
            &mut C,
            Completion<Publication<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>,
            &mut driver::Context<'_, 'd>,
        ),
    ) {
        let (token, result) = done.into_parts();
        let Ok(key) = self.keys.parse(token) else { return };
        let Some(mut sender) = self.sender(key.connection()) else { return };
        let (event, space) = {
            let (flights, connection, _, mut queue) = sender.entry.reborrow().into_transmit();
            let Some(inflight) = connection.observe_send(token, |slot| before(context, slot)) else {
                return;
            };
            let origin = inflight.origin();
            let conn = &mut connection.control;
            let (result, space) = 'native: {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        conn.discard();
                        break 'native (Err(SendOutcome::Failed {
                            failure: origin.failure(error),
                        }), send::Availability::Unchanged);
                    }
                };
                let Some(fd) = conn.engine.establish.fd() else {
                    conn.discard();
                    break 'native (Err(SendOutcome::Close), send::Availability::Unchanged);
                };
                let sent = match inflight.try_complete(&mut conn.engine.operations.lifecycle, bytes) {
                    Ok(done) => done,
                    Err(error) => {
                        conn.discard();
                        break 'native (Err(SendOutcome::Invalid { error: error.into() }),
                            send::Availability::Unchanged);
                    }
                };
                let source = match origin {
                    SendSource::Direct => sending::Source::Direct,
                    SendSource::Queued => sending::Source::Queued,
                    SendSource::Terminal => sending::Source::Terminal,
                    SendSource::WriteShutdown => {
                        wake(conn, origin);
                        break 'native (Ok(sent), send::Availability::Unchanged);
                    }
                };
                let done = send::Completion::from_sent(send::Storage::new(&mut conn.send), sent);
                let wire = send::Context::new(&mut conn.wire, done);
                let (submit, avail, captured) = match conn.engine.send(&mut conn.terminal) {
                    terminal::Admission::Open(end) => {
                        let done = <W::Send as send::Policy<W>>::after_send::<ID>(wire, end);
                        let (next, captured) = done.into_parts();
                        let (ready, avail) = next.into_parts();
                        let op = conn.engine.operations.submission(source);
                        let submit = op.submit(fd, flights, driver, key.target(), ready.resolve(captured));
                        (submit, avail, captured)
                    }
                    terminal::Admission::Closed => {
                        let next = <W::Send as send::Policy<W>>::settle::<ID>(wire, conn.terminal.drain());
                        let (ready, avail) = next.into_parts();
                        if conn.engine.operations.lifecycle.close.is_discarded() {
                            break 'native (Err(SendOutcome::Close), avail);
                        }
                        let op = conn.engine.operations.submission(source);
                        let submit = op.submit(fd, flights, driver, key.target(), ready.close_after());
                        (submit, avail, terminal::Captured::None)
                    }
                };
                let handoff = outcome::Handoff::from_attempt(submit);
                conn.engine.captured(captured);
                if let outcome::Progress::Rejected(error) = handoff.progress() {
                    conn.discard();
                    break 'native (Err(SendOutcome::Rejected { failure: origin.failure(error) }), avail);
                }
                if let terminal::Admission::Open(end) = conn.engine.send(&mut conn.terminal) {
                    let Some(fd) = conn.engine.establish.fd() else {
                        conn.discard();
                        break 'native (Err(SendOutcome::Close), avail);
                    };
                    let op = conn.engine.operations.submission(source);
                    let (captured, handoff) = graceful::submit_pending(
                        send::Context::new(&mut conn.wire, send::Storage::new(&mut conn.send)),
                        op, fd, flights, driver, key.target(), terminal::Admission::Open(end),
                    );
                    conn.engine.captured(captured);
                    if let outcome::Progress::Rejected(error) = handoff.progress() {
                        conn.discard();
                        break 'native (Err(SendOutcome::Rejected { failure: origin.failure(error) }), avail);
                    }
                }
                if conn.engine.operations.sending.is_inflight()
                    || conn.engine.operations.lifecycle.is_waiting()
                {
                    break 'native (Err(SendOutcome::Pending), avail);
                }
                wake(conn, origin);
                (Ok(sent), avail)
            };
            let event = match result {
                Err(SendOutcome::Pending) => Ok(SendOutcome::Pending),
                Err(SendOutcome::Invalid { error }) => {
                    connection.sending().abort_queue(&mut queue);
                    Ok(SendOutcome::Invalid { error })
                }
                result if origin == SendSource::Direct => {
                    if queue.is_active() || connection.sending().has_ack() {
                        connection.sending().abort_queue(&mut queue);
                        Ok(match result {
                            Ok(_) | Err(SendOutcome::Close) => {
                                connection.discard();
                                SendOutcome::Close
                            }
                            Err(error) => error,
                        })
                    } else {
                        Err(match result {
                            Ok(sent) => DirectStatus::Sent(
                                match <<W::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct() {
                                    reclaim::Direct::Independent => None,
                                    reclaim::Direct::Retained => Some(Consumed::proven(sent.into_len())),
                                },
                            ),
                            Err(SendOutcome::Rejected { failure: SendFailure::Direct(error) }) => DirectStatus::Rejected(error),
                            Err(SendOutcome::Failed { failure: SendFailure::Direct(error) }) => DirectStatus::Failed(error),
                            Err(SendOutcome::Close) => DirectStatus::Closed,
                            _ => unreachable!("direct"),
                        })
                    }
                }
                Ok(sent) if origin == SendSource::Queued => {
                    let (native, state) = driver.reborrow().into_regions();
                    Ok(match connection.sending().settle(
                        &mut queue, native, key.connection(), sent,
                        |key, body| release(context, key, body, state),
                    ) {
                        Ok(acknowledged) => SendOutcome::Queued { acknowledged },
                        Err(error) => {
                            connection.sending().abort_queue(&mut queue);
                            connection.discard();
                            SendOutcome::Invalid { error }
                        }
                    })
                }
                Ok(_) => Ok(match origin {
                    SendSource::Terminal => SendOutcome::Terminal,
                    SendSource::WriteShutdown => SendOutcome::WriteShutdown,
                    SendSource::Direct | SendSource::Queued => unreachable!("origin"),
                }),
                Err(error) => {
                    if origin == SendSource::Queued {
                        connection.sending().abort_queue(&mut queue);
                    }
                    Ok(match (origin, error) {
                        (SendSource::Terminal, SendOutcome::Close) => SendOutcome::Terminal,
                        (_, error) => error,
                    })
                }
            };
            (event, space)
        };
        let owner = Publication { sender, space };
        complete(context, match event {
            Ok(outcome) => Completion::Ready(owner, outcome),
            Err(status) => Completion::Direct(Direct { owner, status }),
        }, driver);
    }
}

fn wake<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec>(
    conn: &mut types::Control<'w, 'd, ID, W>, origin: SendSource,
) {
    if origin == SendSource::Terminal {
        if !conn.send.as_slice().is_empty() {
            conn.engine.ready_handle().activate();
        } else {
            transmit::Sending::finish_end(conn);
        }
    } else if conn.terminal.has_send() {
        conn.engine.ready_handle().activate();
    }
}
