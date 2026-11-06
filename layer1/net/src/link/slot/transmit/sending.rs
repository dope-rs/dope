use dope_core::{
    driver::{
        self,
        result::SubmitCause,
        retained::flight,
        route::{self, Identity, kind},
    },
    io::transfer,
};
use o3::base::cell::region;
use outcome::Terminal;
use send::{Store as _, reclaim};

use crate::{
    link::{
        duplex::{sending, state},
        egress::{self, data, reserve::demand},
        event,
        event::SendSource,
        pool,
        slot::{
            transmit::{Finalize, Invalid, graceful, outcome},
            types,
        },
    },
    wire::{self, send, terminal},
};

pub struct Sending<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w> {
    pub(super) slot: &'a mut types::Control<'w, 'd, ID, W>,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w> Sending<'a, 'w, 'd, ID, W> {
    pub(in crate::link::slot) fn new(slot: &'a mut types::Control<'w, 'd, ID, W>) -> Self {
        Self { slot }
    }

    fn target(&self) -> route::Target<'d, ID> {
        self.slot.key().target()
    }

    pub fn discard(&mut self) {
        self.slot.engine.discard(&mut self.slot.terminal);
    }

    pub(super) fn finish_end(slot: &mut types::Control<'w, 'd, ID, W>) {
        let kind = slot.terminal.kind();
        match slot.terminal.finish_send() {
            terminal::Wake::Idle => {}
            terminal::Wake::Ready => slot.engine.ready_handle().activate(),
        }
        if slot.terminal.is_done() {
            match kind {
                Some(terminal::Kind::Clean) => {
                    slot.engine.operations.lifecycle.close.seal();
                    slot.engine.operations.lifecycle.drain();
                }
                Some(terminal::Kind::Failed) | None => {
                    slot.engine.operations.lifecycle.close();
                }
            }
        }
    }

    /// Drains terminal control with one bounded idle fallback.
    pub fn finish(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Finalize<()> {
        let target = self.target();
        let slot = &mut *self.slot;
        if !slot.terminal.has_send() {
            return Finalize::Settled;
        }
        if slot.engine.operations.lifecycle.close.is_discarded() {
            Self::finish_end(slot);
            return Finalize::Abandoned;
        }
        if slot.engine.operations.sending.is_inflight() {
            return Finalize::Armed(());
        }
        match slot.engine.operations.lifecycle.close.request() {
            state::Request::Ready => {}
            state::Request::Discarded => {
                Self::finish_end(slot);
                return Finalize::Abandoned;
            }
            state::Request::Duplicate => {}
        }
        match graceful::flush(slot, slots, driver, target, sending::Source::Terminal) {
            outcome::Progress::Submitted => Finalize::Armed(()),
            outcome::Progress::Idle if slot.engine.operations.lifecycle.is_waiting() => {
                Finalize::Armed(())
            }
            outcome::Progress::Rejected(error) => {
                slot.discard();
                Finalize::Rejected(event::SendFailure::Terminal(error))
            }
            outcome::Progress::Idle => {
                if !slot.send.as_slice().is_empty() {
                    slot.discard();
                    Finalize::Abandoned
                } else {
                    Self::finish_end(slot);
                    Finalize::Settled
                }
            }
        }
    }

    pub(in crate::link) fn settle<'p, 'queue, const IOV: usize, B, D>(
        &mut self,
        queue: &mut egress::Output<'queue, 'p, 'd, IOV, B, D>,
        token: &mut region::Token<region::Identity<'d>>,
        key: pool::Key<'d, ID>,
        sent: send::Sent,
        mut release: impl FnMut(pool::Key<'d, ID>, B),
    ) -> Result<transfer::Len, Invalid>
    where
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        let done = self.slot.engine.operations.queue_done()?;
        match done {
            reclaim::Completion::Submitted(bytes) => {
                if queue.is_active() {
                    Err(Invalid::SettleActive)
                } else {
                    Ok(bytes)
                }
            }
            reclaim::Completion::Retained => {
                let bytes = sent.into_len();
                match queue.complete(
                    token,
                    key.target().operation::<kind::Send>().erase_route(),
                    bytes,
                    |body| release(key, body),
                ) {
                    egress::Change::Applied => Ok(bytes),
                    egress::Change::Stale => Err(Invalid::SettleStale),
                    egress::Change::Invalid(error) => Err(Invalid::Egress(error)),
                }
            }
        }
    }

    pub(in crate::link) fn abort_queue<const IOV: usize, B, D: demand::Mode>(
        &mut self,
        queue: &mut egress::Output<'_, '_, 'd, IOV, B, D>,
        key: pool::Key<'d, ID>,
    ) {
        let acks = &mut self.slot.engine.operations.acknowledgements;
        let held = matches!(
            <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::abort(acks),
            reclaim::Abort::Retained
        );
        if held {
            queue.abort(key.target().operation::<kind::Send>().erase_route());
        }
    }

    pub(in crate::link) fn has_ack(&self) -> bool {
        <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::has_pending(
            &self.slot.engine.operations.acknowledgements,
        )
    }

    pub fn flush_queue(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> outcome::Progress {
        let target = self.target();
        graceful::flush(self.slot, slots, driver, target, sending::Source::Queued)
    }

    pub fn flush_direct(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> outcome::Progress {
        let target = self.target();
        graceful::flush(self.slot, slots, driver, target, sending::Source::Direct)
    }

    pub fn seal(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Finalize<outcome::Wait<'d, ID>> {
        let target = self.target();
        let slot = &mut *self.slot;
        let Some(fd) = slot.engine.establish.fd() else {
            return Finalize::Settled;
        };
        match slot.engine.operations.lifecycle.close.request() {
            state::Request::Ready | state::Request::Duplicate => {}
            state::Request::Discarded => return Finalize::Abandoned,
        }
        let progress = if matches!(
            slot.engine.operations.lifecycle.close.check_seal(),
            state::Seal::Duplicate
        ) {
            graceful::flush(slot, slots, driver, target, sending::Source::Terminal)
        } else {
            let end = slot.engine.send(&mut slot.terminal);
            let submit = slot.engine.operations.submission(sending::Source::Terminal);
            let (captured, handoff) = graceful::submit_pending(
                send::Context::new(&mut slot.wire, send::Storage::new(&mut slot.send)),
                submit,
                fd,
                slots,
                driver,
                target,
                end,
            );
            slot.engine.captured(captured);
            handoff.progress()
        };
        if let outcome::Progress::Rejected(error) = progress {
            if matches!(error.cause(), SubmitCause::IdentityExhausted) {
                slot.discard();
            }
            return Finalize::Rejected(event::SendFailure::Terminal(error));
        }
        if slot.engine.operations.sending.is_inflight()
            || slot.engine.operations.lifecycle.is_waiting()
        {
            slot.engine.operations.lifecycle.close();
            return Finalize::Armed(outcome::Wait::new(slot));
        }
        Finalize::Settled
    }
}

impl<'sending, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w> Sending<'sending, 'w, 'd, ID, W>
where
    'd: 'w,
{
    pub(in crate::link) fn sent(
        self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
        bytes: transfer::Len,
        key: pool::Key<'d, ID, kind::Send>,
    ) -> (Terminal, send::Availability) {
        let target = key.target();
        let conn = &mut *self.slot;
        let Some(fd) = conn.engine.establish.fd() else {
            let origin = match conn.engine.operations.finish_send() {
                sending::Done::Active(origin) => origin,
                sending::Done::Idle => return (Terminal::Drop, send::Availability::Unchanged),
            };
            conn.discard();
            return (Terminal::closed(origin), send::Availability::Unchanged);
        };
        let done = match conn.engine.operations.try_complete_send(bytes) {
            Ok(done) => done,
            Err(error) => {
                conn.discard();
                return (Terminal::Invalid(error), send::Availability::Unchanged);
            }
        };
        let (sent, origin) = done.into_parts();
        let source = match origin {
            SendSource::Direct => sending::Source::Direct,
            SendSource::Queued => sending::Source::Queued,
            SendSource::Terminal => sending::Source::Terminal,
            SendSource::WriteShutdown => {
                Self::wake(conn, origin);
                return (Terminal::sent(origin, sent), send::Availability::Unchanged);
            }
        };
        let done = send::Completion::from_sent(send::Storage::new(&mut conn.send), sent);
        let context = send::Context::new(&mut conn.wire, done);
        let (submit, avail, captured) = match conn.engine.send(&mut conn.terminal) {
            terminal::Admission::Open(end) => {
                let done = <W::Send as send::Policy<W>>::after_send::<ID>(context, end);
                let (next, captured) = done.into_parts();
                let (ready, avail) = next.into_parts();
                let op = conn.engine.operations.submission(source);
                let ready = ready.resolve(captured);
                let submit = op.submit(fd, slots, driver, target, ready);
                (submit, avail, captured)
            }
            terminal::Admission::Closed => {
                let end = conn.terminal.drain();
                let next = <W::Send as send::Policy<W>>::settle::<ID>(context, end);
                let (ready, avail) = next.into_parts();
                if conn.engine.operations.lifecycle.close.is_discarded() {
                    return (Terminal::closed(origin), avail);
                }
                let ready = ready.close_after();
                let captured = terminal::Captured::None;
                let op = conn.engine.operations.submission(source);
                let submit = op.submit(fd, slots, driver, target, ready);
                (submit, avail, captured)
            }
        };
        let handoff = outcome::Handoff::from_attempt(submit);
        match handoff.progress() {
            outcome::Progress::Submitted | outcome::Progress::Idle => {}
            outcome::Progress::Rejected(error) => {
                conn.engine.captured(captured);
                conn.discard();
                return (Terminal::rejected(origin, error), avail);
            }
        }
        conn.engine.captured(captured);
        if let terminal::Admission::Open(end) = conn.engine.send(&mut conn.terminal) {
            let Some(fd) = conn.engine.establish.fd() else {
                conn.discard();
                return (Terminal::closed(origin), avail);
            };
            let op = conn.engine.operations.submission(source);
            let (captured, handoff) = graceful::submit_pending(
                send::Context::new(&mut conn.wire, send::Storage::new(&mut conn.send)),
                op,
                fd,
                slots,
                driver,
                target,
                terminal::Admission::Open(end),
            );
            conn.engine.captured(captured);
            if let outcome::Progress::Rejected(error) = handoff.progress() {
                conn.discard();
                return (Terminal::rejected(origin, error), avail);
            }
        }
        if conn.engine.operations.sending.is_inflight()
            || conn.engine.operations.lifecycle.is_waiting()
        {
            return (Terminal::Pending, avail);
        }
        Self::wake(conn, origin);
        (Terminal::sent(origin, sent), avail)
    }

    fn wake(conn: &mut types::Control<'w, 'd, ID, W>, origin: SendSource) {
        if origin == SendSource::Terminal {
            if !conn.send.as_slice().is_empty() {
                conn.engine.ready_handle().activate();
            } else {
                Self::finish_end(conn);
            }
        } else if conn.terminal.has_send() {
            conn.engine.ready_handle().activate();
        }
    }

    pub(in crate::link) fn failed(self, error: dope_core::Errno) -> Terminal {
        let conn = &mut *self.slot;
        let origin = match conn.engine.operations.finish_send() {
            sending::Done::Active(origin) => origin,
            sending::Done::Idle => return Terminal::Drop,
        };
        conn.discard();
        Terminal::failed(origin, error)
    }
}
