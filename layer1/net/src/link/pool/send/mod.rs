use std::marker::PhantomData;

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
        event::{SendOutcome, SendSource},
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

/// An admitted entry, its one-time availability, or its receipt for the same completion.
pub struct Settlement<'turn, T = ()> {
    value: T,
    scope: PhantomData<fn(&'turn ()) -> &'turn ()>,
}

impl<T> Settlement<'_, T> {
    fn new(value: T) -> Self {
        Self { value, scope: PhantomData }
    }
}

impl<'turn, 'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Settlement<'turn, pool::Entry<'turn, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    pub fn connection(&mut self) -> &mut types::Connection<'w, 'd, ID, W, S> {
        self.value.get_mut()
    }

    fn retire(&mut self, retire: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>)) {
        let pending = self.value.pending();
        let connection = self.value.get_mut();
        retire(connection);
        pending.wait_send(connection.send_status().is_waiting());
    }

    #[inline(always)]
    pub fn finish(
        &mut self,
        space: Settlement<'turn, send::Availability>,
        retire: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>),
    ) -> (&mut transmit::Sender<'turn, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>, Settlement<'turn>) {
        self.retire(retire);
        let sender = &mut self.value.sender;
        <M::Ingress as input::Dispatch<W>>::release::<ID>(space.value, &mut sender.root.runtime);
        if space.value == send::Availability::Released {
            sender.root.pending.release_send();
        }
        (sender, Settlement::new(()))
    }

    pub fn entry(&mut self, _: &Settlement<'turn>) -> &mut pool::Entry<'turn, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        &mut self.value
    }

    pub fn into_entry(self, _: &Settlement<'turn>) -> pool::Entry<'turn, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        self.value
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
    pub fn settle<C, O>(
        &mut self,
        done: Native<'d>,
        driver: &mut driver::Context<'_, 'd>,
        context: &mut C,
        before: impl FnOnce(&mut C, &mut types::Connection<'w, 'd, ID, W, S>),
        mut release: impl FnMut(
            &mut C, pool::Key<'d, ID>, B, &mut region::Token<region::Identity<'d, 1>>,
        ),
        complete: impl for<'turn> FnOnce(
            &mut C,
            SendOutcome,
            Settlement<'turn, pool::Entry<'turn, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>,
            Settlement<'turn, send::Availability>,
            &mut driver::Context<'_, 'd>,
        ) -> (Settlement<'turn>, O),
    ) -> Option<O> {
        let (token, result) = done.into_parts();
        let key = self.keys.parse(token).ok()?;
        let mut settlement = Settlement::new(self.sender(key.connection())?.into_entry());
        let (outcome, space) = {
            let (flights, connection, _, mut queue) = settlement.value.sender.transmit();
            let inflight = connection.observe_send(token, |slot| before(context, slot))?;
            let origin = inflight.source();
            let mut space = send::Availability::Unchanged;
            let event = 'settled: {
                let conn = &mut connection.control;
                let failure = 'wire: {
                    let bytes = match result {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            conn.discard();
                            break 'wire SendOutcome::Failed { failure: SendSource::from(origin).failure(error) };
                        }
                    };
                    let Some(fd) = conn.engine.establish.fd() else {
                        conn.discard();
                        break 'wire SendOutcome::Close;
                    };
                    let sent = match inflight.try_complete(&mut conn.engine.operations.lifecycle, bytes) {
                        Ok(sent) => sent,
                        Err(error) => {
                            conn.discard();
                            connection.sending().abort_queue(&mut queue);
                            break 'settled SendOutcome::Invalid { error: error.into() };
                        }
                    };
                    let Some(source) = origin else {
                        wake(conn, origin);
                        break 'settled SendOutcome::WriteShutdown;
                    };
                    let done = send::Completion::from_sent(send::Storage::new(&mut conn.send), sent);
                    let wire = send::Context::new(&mut conn.wire, done);
                    let (submit, captured) = match conn.engine.send(&mut conn.terminal) {
                        terminal::Admission::Open(end) => {
                            let done = <W::Send as send::Policy<W>>::after_send::<ID>(wire, end);
                            let (next, captured) = done.into_parts();
                            let (ready, avail) = next.into_parts();
                            space = avail;
                            let op = conn.engine.operations.submission(source);
                            (op.submit(fd, flights, driver, key.target(), ready.resolve(captured)), captured)
                        }
                        terminal::Admission::Closed => {
                            let next = <W::Send as send::Policy<W>>::settle::<ID>(wire, conn.terminal.drain());
                            let (ready, avail) = next.into_parts();
                            space = avail;
                            if conn.engine.operations.lifecycle.close.is_discarded() {
                                break 'wire SendOutcome::Close;
                            }
                            let op = conn.engine.operations.submission(source);
                            (op.submit(fd, flights, driver, key.target(), ready.close_after()), terminal::Captured::None)
                        }
                    };
                    let handoff = outcome::Handoff::from_attempt(submit);
                    conn.engine.captured(captured);
                    if let outcome::Progress::Rejected(error) = handoff.progress() {
                        conn.discard();
                        break 'wire SendOutcome::Rejected { failure: SendSource::from(origin).failure(error) };
                    }
                    if let terminal::Admission::Open(end) = conn.engine.send(&mut conn.terminal) {
                        let Some(fd) = conn.engine.establish.fd() else {
                            conn.discard();
                            break 'wire SendOutcome::Close;
                        };
                        let op = conn.engine.operations.submission(source);
                        let (captured, handoff) = graceful::submit_pending(
                            send::Context::new(&mut conn.wire, send::Storage::new(&mut conn.send)),
                            op, fd, flights, driver, key.target(), terminal::Admission::Open(end),
                        );
                        conn.engine.captured(captured);
                        if let outcome::Progress::Rejected(error) = handoff.progress() {
                            conn.discard();
                            break 'wire SendOutcome::Rejected { failure: SendSource::from(origin).failure(error) };
                        }
                    }
                    if conn.engine.operations.sending.is_inflight() || conn.engine.operations.lifecycle.is_waiting() {
                        break 'settled SendOutcome::Pending;
                    }
                    wake(conn, origin);
                    break 'settled match source {
                        sending::Source::Direct if connection.sending().abort_conflict(&mut queue) => {
                            connection.discard();
                            SendOutcome::Close
                        }
                        sending::Source::Direct => SendOutcome::Direct {
                            consumed: match <<W::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct() {
                                reclaim::Direct::Independent => None,
                                reclaim::Direct::Retained => Some(Consumed::proven(sent.into_len())),
                            },
                        },
                        sending::Source::Queued => {
                            let (native, state) = driver.reborrow().into_regions();
                            match connection.sending().settle(
                                &mut queue, native, key.connection(), sent,
                                |key, body| release(context, key, body, state),
                            ) {
                                Ok(acknowledged) => SendOutcome::Queued { acknowledged },
                                Err(error) => {
                                    connection.sending().abort_queue(&mut queue);
                                    connection.discard();
                                    SendOutcome::Invalid { error }
                                }
                            }
                        }
                        sending::Source::Terminal => SendOutcome::Terminal,
                    };
                };
                match (origin, failure) {
                    (Some(sending::Source::Queued), failure) => {
                        connection.sending().abort_queue(&mut queue);
                        failure
                    }
                    (Some(sending::Source::Direct), failure) if connection.sending().abort_conflict(&mut queue) => {
                        if matches!(failure, SendOutcome::Close) {
                            connection.discard();
                        }
                        failure
                    }
                    (Some(sending::Source::Direct), SendOutcome::Close) => SendOutcome::DirectClosed,
                    (Some(sending::Source::Terminal), SendOutcome::Close) => SendOutcome::Terminal,
                    (_, failure) => failure,
                }
            };
            (event, space)
        };
        let (_, output) = complete(context, outcome, settlement, Settlement::new(space), driver);
        Some(output)
    }
}

fn wake<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec>(
    conn: &mut types::Control<'w, 'd, ID, W>, origin: Option<sending::Source>,
) {
    if origin == Some(sending::Source::Terminal) {
        if !conn.send.as_slice().is_empty() {
            conn.engine.ready_handle().activate();
        } else {
            transmit::Sending::finish_end(conn);
        }
    } else if conn.terminal.has_send() {
        conn.engine.ready_handle().activate();
    }
}
