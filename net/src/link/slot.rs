use std::cell::Cell;

use dope_core::backend::Sqe;
use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::kind::RECV;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::driver::{DriverContext, DriverRef};
use dope_core::io::recv::Lease;
use o3::buffer::{Pooled, Shared};
use o3::cell::RegionToken;
use o3::collections::CellQueue;
use o3::marker::ThreadBound;

use super::raw::core::{Core, RecvError, Submit};
use super::raw::pool::rearm::RearmToken;
use crate::link::egress::StableBytes;
use crate::link::egress::queue::Queue;
use crate::link::egress::stable::private;
use crate::link::raw::event::{SendCompletion, SendOutcome};
use crate::wire::send::{Plain, Storage, Vectored};
use crate::wire::{Reclaim, Wire};

const DEFERRED_IOV: usize = 32;

pub const PEND_EGRESS: u8 = 1;
pub const PEND_CLOSE: u8 = 2;

#[derive(Default)]
pub struct PendingFlags {
    flags: Cell<u8>,
    _thread: ThreadBound,
}

impl PendingFlags {
    pub fn contains(&self, flag: u8) -> bool {
        self.flags.get() & flag != 0
    }

    pub fn mark(&self, flag: u8) -> bool {
        let was_clean = self.flags.get() == 0;
        self.flags.set(self.flags.get() | flag);
        was_clean
    }

    pub fn take_flags(&self) -> u8 {
        self.flags.take()
    }
}

pub struct PendingQueue {
    entries: CellQueue<SlotIndex>,
}

impl PendingQueue {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: CellQueue::with_capacity(capacity),
        }
    }

    pub fn mark(&self, index: SlotIndex, pending: &PendingFlags, flag: u8) {
        if pending.mark(flag) {
            let Ok(()) = self.entries.push_back(index) else {
                unreachable!()
            };
        }
    }

    pub fn pop(&self) -> Option<SlotIndex> {
        self.entries.pop_front()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub enum SendBuffer {
    Static(&'static [u8]),
    Shared(Shared),
    Pooled(Pooled),
}

impl AsRef<[u8]> for SendBuffer {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Static(bytes) => bytes,
            Self::Shared(bytes) => bytes.as_ref(),
            Self::Pooled(bytes) => bytes.as_ref(),
        }
    }
}

// SAFETY: Every variant owns immutable retained storage.
unsafe impl private::Sealed for SendBuffer {}

impl From<Shared> for SendBuffer {
    fn from(bytes: Shared) -> Self {
        Self::Shared(bytes)
    }
}

impl From<Pooled> for SendBuffer {
    fn from(bytes: Pooled) -> Self {
        Self::Pooled(bytes)
    }
}

pub struct DeferredEgress {
    close_after: Cell<bool>,
}

impl DeferredEgress {
    pub fn new() -> Self {
        Self {
            close_after: Cell::new(false),
        }
    }

    pub fn new_for() -> Self {
        Self::new()
    }

    pub fn stage<'d>(
        &self,
        token: &mut RegionToken<'d>,
        queue: &Queue<'_, 'd, '_, DEFERRED_IOV, SendBuffer>,
        bytes: Shared,
        close: bool,
    ) -> bool {
        self.stage_buffer(token, queue, bytes.into(), close)
    }

    pub fn stage_buffer<'d>(
        &self,
        token: &mut RegionToken<'d>,
        queue: &Queue<'_, 'd, '_, DEFERRED_IOV, SendBuffer>,
        bytes: SendBuffer,
        close: bool,
    ) -> bool {
        let committed = match bytes {
            SendBuffer::Static(bytes) => queue.try_enqueue_static(token, bytes),
            bytes => queue.try_enqueue(token, bytes).is_ok(),
        };
        if committed {
            self.close_after.set(self.close_after.get() | close);
        }
        committed
    }

    pub fn stage_copy<'d>(
        &mut self,
        token: &mut RegionToken<'d>,
        queue: &mut Queue<'_, 'd, '_, DEFERRED_IOV, SendBuffer>,
        bytes: &[u8],
        close: bool,
    ) -> bool {
        self.stage_copy_pair(token, queue, bytes, None, close)
    }

    pub fn stage_copy_pair<'d>(
        &mut self,
        token: &mut RegionToken<'d>,
        queue: &mut Queue<'_, 'd, '_, DEFERRED_IOV, SendBuffer>,
        first: &[u8],
        second: Option<SendBuffer>,
        close: bool,
    ) -> bool {
        let committed = match second {
            Some(SendBuffer::Static(bytes)) => queue.try_enqueue_copy_static(token, first, bytes),
            second => queue.try_enqueue_copy_pair(token, first, second),
        };
        if committed {
            self.close_after.set(self.close_after.get() | close);
        }
        committed
    }

    pub fn is_idle(&self, queue: &Queue<'_, '_, '_, DEFERRED_IOV, SendBuffer>) -> bool {
        queue.total_bytes() == 0
    }

    pub fn close_after(&self) -> bool {
        self.close_after.get()
    }

    pub fn try_ack<'d>(
        &mut self,
        token: &mut RegionToken<'d>,
        queue: &mut Queue<'_, 'd, '_, DEFERRED_IOV, SendBuffer>,
        n: usize,
    ) -> bool {
        queue.try_ack(token, n)
    }
}

impl Default for DeferredEgress {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Slot<'d, W: Wire, S> {
    pub(in crate::link) core: Core<'d>,
    pub(in crate::link) wire: W::Connection<'d>,
    pub(in crate::link) send: W::SendStorage,
    pub state: S,
    token: RearmToken,
}

impl<'d, W: Wire, S> Slot<'d, W, S> {
    pub(in crate::link) fn new(
        core: Core<'d>,
        wire: W::Connection<'d>,
        send: W::SendStorage,
        token: RearmToken,
        state: S,
    ) -> Self {
        Self {
            core,
            wire,
            send,
            state,
            token,
        }
    }

    pub fn token(&self) -> Token {
        self.token.token()
    }

    pub(in crate::link) fn rearm_token(&self) -> RearmToken {
        self.token
    }

    pub fn close(self, driver: &mut DriverContext<'_, 'd>) {
        let guard = driver.guard(self.core.into_fd());
        drop(guard);
    }

    pub fn is_send_inflight(&self) -> bool {
        self.core.is_send_inflight()
    }

    pub fn is_closing(&self) -> bool {
        self.core.is_closing()
    }

    pub fn begin_close(&mut self) {
        self.core.begin_close();
    }

    pub fn should_close(&self, defer: bool) -> bool {
        self.core.should_close(defer)
    }

    pub fn mark_aborted(&mut self) {
        self.core.mark_aborted();
    }

    pub fn holds_plain(&self) -> bool {
        W::holds_plain(&self.wire, &self.send)
    }

    #[doc(hidden)]
    pub fn close_io_state(&self) -> (bool, bool, bool, u8) {
        (
            self.core.is_send_inflight(),
            self.core.is_armed(),
            self.core.is_closing(),
            self.core.recv_cancel_kind(),
        )
    }

    pub fn set_close_after(&mut self) {
        self.core.set_close_after();
    }

    pub fn close_after(&self) -> bool {
        self.core.close_after()
    }

    pub fn mark_ready(&self) {
        self.core.fd.ready_handle().activate();
    }

    #[doc(hidden)]
    pub fn ready_key(&self) -> ReadyKey<'d> {
        self.core.fd.ready_handle().key()
    }

    #[doc(hidden)]
    pub fn driver(&self) -> DriverRef<'d> {
        self.core.fd.driver()
    }

    fn finish_submit(wire: &mut W::Connection<'d>, submit: Submit) -> usize {
        match submit {
            Submit::Submitted(consumed) => consumed,
            Submit::Rejected(consumed) => {
                W::submit_failed(wire);
                if matches!(W::RECLAIM, Reclaim::OnSubmit) {
                    consumed
                } else {
                    0
                }
            }
            Submit::Idle(consumed) => {
                if matches!(W::RECLAIM, Reclaim::OnSubmit) {
                    consumed
                } else {
                    0
                }
            }
        }
    }

    pub fn submit_plain(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        plain: Plain<'_>,
        ud: Token,
    ) -> usize {
        if self.core.is_send_inflight() {
            return 0;
        }
        let limit = plain.len();
        let prepared = W::prepare_send(&mut self.wire, Storage::new(&mut self.send, limit), plain);
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit)
    }

    #[doc(hidden)]
    pub fn submit_vectored(
        &mut self,
        plain: Vectored<'_>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> usize {
        if self.core.is_send_inflight() {
            return 0;
        }
        let limit = plain.bytes();
        let prepared =
            W::prepare_send_vectored(&mut self.wire, Storage::new(&mut self.send, limit), plain);
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit)
    }

    /// Prepares and submits one queue-backed send.
    pub fn submit_egress<const IOV: usize, B: StableBytes>(
        &mut self,
        queue: &mut Queue<'_, 'd, '_, IOV, B>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> usize {
        if self.core.is_send_inflight() || queue.is_send_inflight() {
            return 0;
        }
        let submit = {
            let Some(plain) = queue.prepare_flight(driver.region_token(), u32::MAX as usize) else {
                self.flush_pending(driver, ud);
                return 0;
            };
            let limit = plain.bytes();
            let prepared = W::prepare_send_vectored(
                &mut self.wire,
                Storage::new(&mut self.send, limit),
                plain,
            );
            self.core.submit_prepared(driver, ud, prepared)
        };
        match submit {
            Submit::Submitted(consumed) => {
                if matches!(W::RECLAIM, Reclaim::OnComplete) {
                    queue.mark_flight(ud);
                } else if !queue.settle_flight(driver.region_token(), consumed)
                    || !queue.record_submitted_plain(consumed)
                {
                    self.core.mark_aborted();
                    self.core.begin_close();
                    return 0;
                }
                consumed
            }
            Submit::Rejected(consumed) => {
                W::submit_failed(&mut self.wire);
                if matches!(W::RECLAIM, Reclaim::OnSubmit) {
                    if !queue.settle_flight(driver.region_token(), consumed)
                        || !queue.record_submitted_plain(consumed)
                    {
                        self.core.mark_aborted();
                        self.core.begin_close();
                        return 0;
                    }
                    consumed
                } else {
                    if !queue.settle_flight(driver.region_token(), 0) {
                        self.core.mark_aborted();
                        self.core.begin_close();
                    }
                    0
                }
            }
            Submit::Idle(consumed) => {
                if matches!(W::RECLAIM, Reclaim::OnSubmit) {
                    if !queue.settle_flight(driver.region_token(), consumed)
                        || !queue.record_submitted_plain(consumed)
                    {
                        self.core.mark_aborted();
                        self.core.begin_close();
                        return 0;
                    }
                    consumed
                } else {
                    if !queue.settle_flight(driver.region_token(), 0) {
                        self.core.mark_aborted();
                        self.core.begin_close();
                    }
                    0
                }
            }
        }
    }

    pub fn complete_egress<const IOV: usize, B: StableBytes>(
        &mut self,
        queue: &mut Queue<'_, 'd, '_, IOV, B>,
        token: &mut RegionToken<'d>,
        completion: SendCompletion,
    ) -> Result<usize, SendCompletion> {
        if matches!(W::RECLAIM, Reclaim::OnSubmit) {
            return Ok(queue.take_submitted_plain());
        }
        let bytes = completion.bytes();
        if !queue.complete_flight(token, completion.target(), bytes) {
            return Err(completion);
        }
        Ok(bytes)
    }

    pub fn abort_egress<const IOV: usize, B: StableBytes>(
        &mut self,
        queue: &mut Queue<'_, 'd, '_, IOV, B>,
        completion: SendCompletion,
    ) -> bool {
        if matches!(W::RECLAIM, Reclaim::OnSubmit) {
            queue.take_submitted_plain();
            true
        } else {
            queue.abort_flight(completion.target())
        }
    }

    pub fn flush_pending(&mut self, driver: &mut DriverContext<'_, 'd>, ud: Token) {
        if self.core.is_send_inflight() {
            return;
        }
        let prepared = W::flush_pending(&mut self.wire, Storage::new(&mut self.send, 0));
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit);
    }

    pub fn seal_graceful(&mut self, driver: &mut DriverContext<'_, 'd>, ud: Token) -> bool {
        if self.core.request_graceful() && self.core.take_graceful() {
            let prepared = W::graceful_close(&mut self.wire, Storage::new(&mut self.send, 0));
            let submit = self.core.submit_prepared(driver, ud, prepared);
            Self::finish_submit(&mut self.wire, submit);
        }
        if self.core.is_send_inflight() {
            self.core.begin_close();
            return true;
        }
        false
    }

    pub fn recv_data<'a>(
        &mut self,
        runtime: &mut W::RuntimeContext<'d>,
        more: bool,
        slice: &'a mut [u8],
    ) -> RecvDecision<W::RecvBatch<'a>> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        let needs_rearm = self.core.recv_data(more);
        let swallowed = self.core.consume_discard(slice.len());
        if swallowed == slice.len() {
            return RecvDecision::Discarded { needs_rearm };
        }
        let chunk = W::process_recv(&mut self.wire, runtime, &mut slice[swallowed..]);
        if chunk.len() == 0 {
            RecvDecision::NoChunk { needs_rearm }
        } else {
            RecvDecision::Chunk { chunk, needs_rearm }
        }
    }

    pub fn recv_retained_data<'a>(
        &mut self,
        runtime: &mut W::RuntimeContext<'d>,
        more: bool,
        mut bytes: Lease<'a>,
    ) -> RecvDecision<W::RetainedRecv<'a>> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        let needs_rearm = self.core.recv_data(more);
        let swallowed = self.core.consume_discard(bytes.as_slice().len());
        if swallowed == bytes.as_slice().len() {
            return RecvDecision::Discarded { needs_rearm };
        }
        bytes.advance(swallowed);
        match W::process_retained_recv(&mut self.wire, runtime, bytes) {
            Some(chunk) => RecvDecision::Chunk { chunk, needs_rearm },
            None => RecvDecision::NoChunk { needs_rearm },
        }
    }

    pub fn recv_discarded<C>(&mut self, len: u32) -> RecvDecision<C> {
        if !self.core.is_armed() || !self.core.is_discard_armed() {
            return RecvDecision::Drop;
        }
        RecvDecision::Discarded {
            needs_rearm: self.core.recv_discarded(len),
        }
    }

    pub fn begin_discard(&mut self, driver: &mut DriverContext<'_, 'd>, n: u64) -> bool {
        if n == 0
            || !W::RAW_RECV
            || !self.core.kernel_discard()
            || !Sqe::SUPPORTS_RECV_DISCARD
            || self.core.discard_remaining() > 0
            || self.core.is_closing()
            || self.core.close_after()
        {
            return false;
        }
        self.core.begin_discard(n);
        if self.core.is_armed() && !self.core.is_discard_armed() {
            let token = self.token;
            Core::push_retry(driver, || Sqe::cancel(token.token(), RECV));
        }
        true
    }

    pub fn recv_eof<C>(&mut self, more: bool) -> RecvDecision<C> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        self.core.recv_eof(more);
        W::recv_eof(&mut self.wire);
        RecvDecision::Close
    }

    pub fn recv_cancelled<C>(&mut self, more: bool) -> RecvDecision<C> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        match self.core.recv_cancelled(more) {
            RecvError::Closed => RecvDecision::Close,
            RecvError::Live { needs_rearm } => RecvDecision::NoChunk { needs_rearm },
        }
    }

    pub fn recv_starved<C>(&mut self, more: bool) -> RecvDecision<C> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        RecvDecision::NoChunk {
            needs_rearm: self.core.recv_starved(more),
        }
    }

    pub fn recv_failed<C>(&mut self, more: bool) -> RecvDecision<C> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        self.core.recv_failed(more);
        RecvDecision::Close
    }

    pub fn send_sent(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        n: u32,
        ud: Token,
        idx: SlotIndex,
    ) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        let Some(sent) = self.core.complete_send(n) else {
            return SendOutcome::Close {
                idx,
                completion: SendCompletion::new(ud, n as usize),
            };
        };
        let prepared = W::after_send(&mut self.wire, Storage::new(&mut self.send, 0), sent);
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit);
        if !self.core.is_send_inflight() && self.core.take_graceful() {
            let prepared = W::graceful_close(&mut self.wire, Storage::new(&mut self.send, 0));
            let submit = self.core.submit_prepared(driver, ud, prepared);
            Self::finish_submit(&mut self.wire, submit);
        }
        if self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        SendOutcome::Sent {
            idx,
            completion: SendCompletion::new(ud, n as usize),
        }
    }

    pub fn send_failed(&mut self, idx: SlotIndex, ud: Token) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        self.core.send_done();
        self.core.mark_aborted();
        self.core.begin_close();
        SendOutcome::Close {
            idx,
            completion: SendCompletion::new(ud, 0),
        }
    }
}

pub enum RecvDecision<C> {
    Drop,
    Close,
    NoChunk { needs_rearm: bool },
    Discarded { needs_rearm: bool },
    Chunk { chunk: C, needs_rearm: bool },
}
