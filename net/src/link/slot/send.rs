use dope_core::{
    driver::{
        flight, retained,
        route::{self, kind},
    },
    io::fd::handles,
};
use o3::cell::region;

use crate::{
    link::{
        self,
        egress::{self, data},
        engine::sending,
        event, pool,
        slot::types,
    },
    wire::{self, reclaim, send},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    Clear,
    Held,
}

/// Immediate outcome of one egress submission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Progress {
    Runnable,
    Waiting,
    Quiescent,
}

const _: () = assert!(std::mem::size_of::<Progress>() == 1);

fn finish<'d, const ID: u8, W: wire::Wire>(
    wire: &mut W::Connection<'d, ID>,
    submit: send::Outcome<W::Reclaim>,
) -> link::Consumed {
    let amount = match submit {
        send::Outcome::Submitted(consumed, _) => consumed,
        send::Outcome::Rejected(consumed, _) => {
            W::submit_failed(wire);
            if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                consumed
            } else {
                0
            }
        }
        send::Outcome::Idle(consumed, _) => {
            if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                consumed
            } else {
                0
            }
        }
    };
    link::Consumed::proven(amount)
}

fn submit_pending_graceful<'d, const ID: u8, W: wire::Wire>(
    wire: &mut W::Connection<'d, ID>,
    storage: &mut W::StorageBackend<'d>,
    mut submission: sending::Submission<'_, 'd>,
    fd: &handles::Descriptor<'d>,
    slots: &flight::Slots<'d, route::KeyTag<ID>>,
    driver: &mut retained::Context<'_, '_, 'd>,
    target: route::Target<'d, route::KeyTag<ID>>,
) {
    if !submission.take_pending_graceful() {
        return;
    }
    let prepared = W::graceful_close(wire, send::Storage::new(storage, 0));
    let submit = submission.submit_prepared(fd, slots, driver, target, prepared);
    finish::<ID, W>(wire, submit);
}

pub struct Status<'a, 'd, const ID: u8, W: wire::Wire, S> {
    connection: &'a types::Connection<'d, ID, W, S>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, S> Status<'a, 'd, ID, W, S> {
    pub(super) fn new(connection: &'a types::Connection<'d, ID, W, S>) -> Self {
        Self { connection }
    }

    pub fn inflight(&self) -> bool {
        self.connection.engine.sending.is_inflight()
    }

    pub fn retention(&self) -> Retention {
        if W::holds_plain(&self.connection.wire, &self.connection.send) {
            Retention::Held
        } else {
            Retention::Clear
        }
    }
}

pub struct Sending<'a, 'd, const ID: u8, W: wire::Wire, S> {
    connection: &'a mut types::Connection<'d, ID, W, S>,
}

#[doc(hidden)]
pub struct DirectSending<'a, 'd, const ID: u8, W: wire::Wire> {
    engine: &'a mut link::Engine<'d>,
    wire: &'a mut W::Connection<'d, ID>,
    storage: &'a mut W::StorageBackend<'d>,
    target: route::Target<'d, route::KeyTag<ID>>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire> DirectSending<'a, 'd, ID, W> {
    pub(super) fn new(
        engine: &'a mut link::Engine<'d>,
        wire: &'a mut W::Connection<'d, ID>,
        storage: &'a mut W::StorageBackend<'d>,
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> Self {
        Self {
            engine,
            wire,
            storage,
            target,
        }
    }

    pub fn inflight(&self) -> bool {
        self.engine.sending.is_inflight()
    }

    pub fn abort(&mut self) {
        self.engine.lifecycle.abort();
    }

    pub fn submit_plain(
        &mut self,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
        plain: send::Plain<'_>,
    ) -> link::Consumed {
        if self.inflight() {
            return link::Consumed::ZERO;
        }
        let Some(fd) = self.engine.establish.fd() else {
            return link::Consumed::ZERO;
        };
        let limit = plain.len();
        let prepared = W::prepare_send(self.wire, send::Storage::new(self.storage, limit), plain);
        let submit = self
            .engine
            .sending
            .submission(&mut self.engine.flights, &mut self.engine.lifecycle)
            .submit_prepared(fd, slots, driver, self.target, prepared);
        finish::<ID, W>(self.wire, submit)
    }

    pub fn submit_vectored(
        &mut self,
        plain: send::Vectored<'_>,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> link::Consumed {
        if self.inflight() {
            return link::Consumed::ZERO;
        }
        let Some(fd) = self.engine.establish.fd() else {
            return link::Consumed::ZERO;
        };
        let limit = plain.bytes();
        let prepared =
            W::prepare_send_vectored(self.wire, send::Storage::new(self.storage, limit), plain);
        let submit = self
            .engine
            .sending
            .submission(&mut self.engine.flights, &mut self.engine.lifecycle)
            .submit_prepared(fd, slots, driver, self.target, prepared);
        finish::<ID, W>(self.wire, submit)
    }
}

impl<'a, 'd, const ID: u8, W: wire::Wire, S> Sending<'a, 'd, ID, W, S> {
    pub(super) fn new(connection: &'a mut types::Connection<'d, ID, W, S>) -> Self {
        Self { connection }
    }

    fn target(&self) -> route::Target<'d, route::KeyTag<ID>> {
        self.connection.key().target()
    }

    pub fn submit_egress<const IOV: usize, B: data::Payload<'d>>(
        &mut self,
        queue: &mut egress::Queue<'_, 'd, IOV, B>,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Progress, link::EgressError> {
        let target = self.target();
        let connection = &mut *self.connection;
        let Some(fd) = connection.engine.establish.fd() else {
            return Ok(Progress::Waiting);
        };
        if connection.engine.sending.is_inflight() || queue.is_send_inflight() {
            return Ok(Progress::Waiting);
        }
        let Some(mut flight) = queue.prepare_flight(driver.region_token(), u32::MAX as usize)
        else {
            return match Self::flush(connection, slots, driver, target) {
                send::Outcome::Submitted(_, _) => Ok(Progress::Waiting),
                send::Outcome::Idle(_, _) => Ok(Progress::Quiescent),
                send::Outcome::Rejected(_, _) => Err(link::EgressError),
            };
        };
        let limit = flight.bytes();
        let submit = {
            let plain = flight.vectored();
            let prepared = W::prepare_send_vectored(
                &mut connection.wire,
                send::Storage::new(&mut connection.send, limit),
                plain,
            );
            connection
                .engine
                .sending
                .submission(
                    &mut connection.engine.flights,
                    &mut connection.engine.lifecycle,
                )
                .submit_prepared(fd, slots, driver, target, prepared)
        };
        match submit {
            send::Outcome::Submitted(consumed, _) => {
                if !<W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                    flight.retain(route::Token::from_target(target).with_kind(kind::SEND));
                } else {
                    let Some(released) = flight.release(consumed) else {
                        connection.engine.lifecycle.abort();
                        return Err(link::EgressError);
                    };
                    if queue
                        .transfer()
                        .settle_submitted(driver.region_token(), released, consumed)
                    {
                        return Ok(Progress::Waiting);
                    }
                    connection.engine.lifecycle.abort();
                    return Err(link::EgressError);
                }
                Ok(Progress::Waiting)
            }
            send::Outcome::Rejected(consumed, _) => {
                W::submit_failed(&mut connection.wire);
                let released = flight.release(if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                    consumed
                } else {
                    0
                });
                if let Some(released) = released {
                    let settled = if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                        queue
                            .transfer()
                            .settle_submitted(driver.region_token(), released, consumed)
                    } else {
                        queue.transfer().settle(driver.region_token(), released)
                    };
                    if !settled {
                        connection.engine.lifecycle.abort();
                        return Err(link::EgressError);
                    }
                } else {
                    connection.engine.lifecycle.abort();
                }
                let _ = consumed;
                connection.engine.lifecycle.abort();
                Err(link::EgressError)
            }
            send::Outcome::Idle(consumed, _) => {
                let released = flight.release(if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                    consumed
                } else {
                    0
                });
                if let Some(released) = released {
                    let settled = if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
                        queue
                            .transfer()
                            .settle_submitted(driver.region_token(), released, consumed)
                    } else {
                        queue.transfer().settle(driver.region_token(), released)
                    };
                    if !settled {
                        connection.engine.lifecycle.abort();
                        return Err(link::EgressError);
                    }
                } else {
                    connection.engine.lifecycle.abort();
                    return Err(link::EgressError);
                }
                if consumed > 0 && queue.total_bytes() > 0 {
                    Ok(Progress::Runnable)
                } else {
                    Ok(Progress::Quiescent)
                }
            }
        }
    }

    pub fn complete_egress<const IOV: usize, B: data::Payload<'d>>(
        &mut self,
        queue: &mut egress::Queue<'_, 'd, IOV, B>,
        token: &mut region::Token<'d>,
        completion: event::SendCompletion<'d, ID>,
    ) -> Result<usize, event::SendCompletion<'d, ID>> {
        if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
            return Ok(queue.transfer().take_submitted());
        }
        let bytes = completion.sent().get();
        if !queue
            .transfer()
            .complete(token, route::Token::from(completion.key()), bytes)
        {
            return Err(completion);
        }
        Ok(bytes)
    }

    pub fn abort_egress<const IOV: usize, B: data::Payload<'d>>(
        &mut self,
        queue: &mut egress::Queue<'_, 'd, IOV, B>,
        completion: event::SendCompletion<'d, ID>,
    ) -> bool {
        if <W::Reclaim as reclaim::Policy>::ON_SUBMIT {
            queue.transfer().take_submitted();
            true
        } else {
            queue.transfer().abort(route::Token::from(completion.key()))
        }
    }

    pub fn flush_pending(
        &mut self,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let target = self.target();
        Self::flush(self.connection, slots, driver, target);
    }

    fn flush(
        connection: &mut types::Connection<'d, ID, W, S>,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> send::Outcome<W::Reclaim> {
        if connection.engine.sending.is_inflight() {
            return send::Outcome::idle(0);
        }
        let Some(fd) = connection.engine.establish.fd() else {
            return send::Outcome::idle(0);
        };
        let prepared = W::flush_pending(
            &mut connection.wire,
            send::Storage::new(&mut connection.send, 0),
        );
        let submit = connection
            .engine
            .sending
            .submission(
                &mut connection.engine.flights,
                &mut connection.engine.lifecycle,
            )
            .submit_prepared(fd, slots, driver, target, prepared);
        finish::<ID, W>(&mut connection.wire, submit);
        if matches!(submit, send::Outcome::Rejected(_, _)) {
            connection.engine.lifecycle.abort();
        }
        submit
    }

    pub fn seal_graceful(
        &mut self,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        let target = self.target();
        let connection = &mut *self.connection;
        let Some(fd) = connection.engine.establish.fd() else {
            return false;
        };
        if connection.engine.lifecycle.request_graceful() {
            let submission = connection.engine.sending.submission(
                &mut connection.engine.flights,
                &mut connection.engine.lifecycle,
            );
            submit_pending_graceful::<ID, W>(
                &mut connection.wire,
                &mut connection.send,
                submission,
                fd,
                slots,
                driver,
                target,
            );
        }
        if connection.engine.sending.is_inflight() {
            connection.engine.lifecycle.begin_close();
            return true;
        }
        false
    }

    pub(in crate::link) fn sent(
        &mut self,
        slots: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
        bytes: u32,
        key: pool::Key<'d, ID>,
    ) -> (event::SendOutcome<'d, ID>, send::Availability) {
        let target = key.target();
        let connection = &mut *self.connection;
        if !connection.engine.sending.is_inflight() {
            return (event::SendOutcome::Drop, send::Availability::Unchanged);
        }
        let Some(fd) = connection.engine.establish.fd() else {
            connection
                .engine
                .sending
                .done(&mut connection.engine.flights);
            connection.engine.lifecycle.abort();
            return (
                event::SendOutcome::Close(event::SendCompletion::new(key, send::Sent::new(0))),
                send::Availability::Unchanged,
            );
        };
        let Some(sent) = connection.engine.sending.complete(
            &mut connection.engine.flights,
            &mut connection.engine.lifecycle,
            bytes,
        ) else {
            return (
                event::SendOutcome::Close(event::SendCompletion::new(key, send::Sent::new(0))),
                send::Availability::Unchanged,
            );
        };
        let after_send = W::after_send(
            &mut connection.wire,
            send::Storage::new(&mut connection.send, 0),
            sent,
        );
        let (prepared, availability) = after_send.into_parts();
        let submit = connection
            .engine
            .sending
            .submission(
                &mut connection.engine.flights,
                &mut connection.engine.lifecycle,
            )
            .submit_prepared(fd, slots, driver, target, prepared);
        finish::<ID, W>(&mut connection.wire, submit);
        let submission = connection.engine.sending.submission(
            &mut connection.engine.flights,
            &mut connection.engine.lifecycle,
        );
        submit_pending_graceful::<ID, W>(
            &mut connection.wire,
            &mut connection.send,
            submission,
            fd,
            slots,
            driver,
            target,
        );
        if connection.engine.sending.is_inflight() {
            return (event::SendOutcome::Drop, availability);
        }
        (
            event::SendOutcome::Sent(event::SendCompletion::new(key, sent)),
            availability,
        )
    }

    pub(in crate::link) fn failed(&mut self, key: pool::Key<'d, ID>) -> event::SendOutcome<'d, ID> {
        let connection = &mut *self.connection;
        if !connection.engine.sending.is_inflight() {
            return event::SendOutcome::Drop;
        }
        connection
            .engine
            .sending
            .done(&mut connection.engine.flights);
        connection.engine.lifecycle.abort();
        event::SendOutcome::Close(event::SendCompletion::new(key, send::Sent::new(0)))
    }
}
