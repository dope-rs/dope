use dope_core::{
    driver::{
        self,
        result::SubmitCause,
        retained::flight,
        route::{self, Identity},
    },
    io::transfer,
};
use o3::base::cell::region;
use send::{Store as _, reclaim};

use crate::{
    link::{
        duplex::{sending, state},
        egress::{self, data, reserve::demand},
        event,
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

    pub(in crate::link) fn finish_end(slot: &mut types::Control<'w, 'd, ID, W>) {
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
        let (acknowledged, bytes) = match done {
            reclaim::Completion::Submitted(bytes) => {
                if !queue.is_active() {
                    return Ok(bytes);
                }
                (bytes, None)
            }
            reclaim::Completion::Retained => {
                let bytes = sent.into_len();
                (bytes, Some(bytes))
            }
        };
        match queue.complete(token, bytes, |body| release(key, body)) {
            egress::Change::Applied => Ok(acknowledged),
            egress::Change::Stale => Err(Invalid::SettleStale),
            egress::Change::Invalid(error) => Err(Invalid::Egress(error)),
        }
    }

    pub(in crate::link) fn abort_queue<
        const IOV: usize,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    >(
        &mut self,
        queue: &mut egress::Output<'_, '_, 'd, IOV, B, D>,
    ) {
        let acks = &mut self.slot.engine.operations.acknowledgements;
        <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::abort(acks);
        queue.abort();
    }

    pub(in crate::link) fn abort_conflict<const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>(
        &mut self, queue: &mut egress::Output<'_, '_, 'd, IOV, B, D>,
    ) -> bool {
        if queue.is_active() || <<W::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::has_pending(
            &self.slot.engine.operations.acknowledgements,
        ) {
            self.abort_queue(queue);
            true
        } else {
            false
        }
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

