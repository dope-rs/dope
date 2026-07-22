use std::cell::Cell;

use o3::buffer::{Pooled, Shared};
use o3::collections::CellQueue;
use o3::marker::ThreadBound;

use super::core::{Core, RecvError, Submit};
use super::egress;

use crate::link::pool::SendOutcome;
use crate::wire::send::{Plain, Storage, Vectored};
use crate::wire::{Reclaim, Wire};
use dope_core::backend::Sqe;
use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::kind::RECV;
use dope_core::driver::token::{SlotIndex, Token};

const DEFERRED_IOV: usize = 32;

pub const PEND_EGRESS: u8 = 1;
pub const PEND_SHUTDOWN: u8 = 2;
pub const PEND_CLOSE: u8 = 4;

#[derive(Default)]
pub struct PendingFlags {
    flags: Cell<u8>,
    shutdown_how: Cell<i32>,
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

    pub fn shutdown_how(&self) -> i32 {
        self.shutdown_how.get()
    }

    pub fn set_shutdown(&self, how: i32) {
        self.shutdown_how.set(how);
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
    queue: egress::queue::Queue<DEFERRED_IOV, SendBuffer>,
    close_after: Cell<bool>,
}

impl DeferredEgress {
    pub fn new() -> Self {
        let arena = egress::arena::Arena::<SendBuffer>::default();
        Self {
            queue: arena.queue_for(0),
            close_after: Cell::new(false),
        }
    }

    pub fn new_for(arena: &egress::arena::Arena<SendBuffer>, lane: usize) -> Self {
        Self {
            queue: arena.queue_for(lane),
            close_after: Cell::new(false),
        }
    }

    pub fn stage(&self, bytes: Shared, close: bool) -> bool {
        self.stage_buffer(bytes.into(), close)
    }

    pub fn stage_buffer(&self, bytes: SendBuffer, close: bool) -> bool {
        let committed = match bytes {
            SendBuffer::Static(bytes) => self.queue.try_enqueue_static(bytes),
            bytes => self.queue.try_enqueue(bytes).is_ok(),
        };
        if committed {
            self.close_after.set(self.close_after.get() | close);
        }
        committed
    }

    pub fn stage_copy(&mut self, bytes: &[u8], close: bool) -> bool {
        self.stage_copy_pair(bytes, None, close)
    }

    pub fn stage_copy_pair(
        &mut self,
        first: &[u8],
        second: Option<SendBuffer>,
        close: bool,
    ) -> bool {
        let committed = match second {
            Some(SendBuffer::Static(bytes)) => self.queue.try_enqueue_copy_static(first, bytes),
            second => self.queue.try_enqueue_copy_pair(first, second),
        };
        if committed {
            self.close_after.set(self.close_after.get() | close);
        }
        committed
    }

    pub fn is_idle(&self) -> bool {
        self.queue.total_bytes() == 0
    }

    pub fn close_after(&self) -> bool {
        self.close_after.get()
    }

    pub fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        self.queue.prepare_send(bytes_cap)
    }

    pub fn ack(&mut self, n: usize) {
        self.queue.ack(n);
    }
}

impl Default for DeferredEgress {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Slot<'d, W: Wire, S> {
    pub core: Core<'d>,
    pub wire: W,
    pub send: W::SendStorage,
    pub state: S,
    token: Token,
}

impl<'d, W: Wire, S> Slot<'d, W, S> {
    pub fn new(core: Core<'d>, wire: W, send: W::SendStorage, token: Token, state: S) -> Self {
        Self {
            core,
            wire,
            send,
            state,
            token,
        }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn close(self, driver: &mut dope_core::driver::DriverContext<'_, 'd>) {
        let guard = driver.guard(self.core.into_fd());
        drop(guard);
    }

    pub fn is_send_inflight(&self) -> bool {
        self.core.is_send_inflight()
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
    pub fn driver(&self) -> dope_core::driver::DriverRef<'d> {
        self.core.fd.driver()
    }

    fn finish_submit(wire: &mut W, submit: Submit) -> usize {
        match submit {
            Submit::Submitted(consumed) => consumed,
            Submit::Rejected(consumed) => {
                wire.submit_failed();
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
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
        plain: &[u8],
        ud: Token,
    ) -> usize {
        if self.core.is_send_inflight() {
            return 0;
        }
        let prepared = self
            .wire
            .prepare_send(Storage::new(&mut self.send, plain.len()), Plain::new(plain));
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit)
    }

    pub fn submit_wire_vectored(
        core: &mut Core<'d>,
        wire: &mut W,
        send: &mut W::SendStorage,
        plain: Vectored<'_>,
        ud: Token,
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
    ) -> usize {
        if core.is_send_inflight() {
            return 0;
        }
        let limit = plain.bytes();
        let prepared = wire.prepare_send_vectored(Storage::new(send, limit), plain);
        let submit = core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(wire, submit)
    }

    pub fn flush_pending(
        &mut self,
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
        ud: Token,
    ) {
        if self.core.is_send_inflight() {
            return;
        }
        let prepared = self.wire.flush_pending(Storage::new(&mut self.send, 0));
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit);
    }

    pub fn seal_graceful(
        &mut self,
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
        ud: Token,
    ) -> bool {
        if self.core.request_graceful() && self.core.take_graceful() {
            let prepared = self.wire.graceful_close(Storage::new(&mut self.send, 0));
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
        runtime: &W::RuntimeContext,
        more: bool,
        slice: &'a [u8],
    ) -> RecvDecision<W::Recv<'a>> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        let needs_rearm = self.core.recv_data(more);
        let swallowed = self.core.consume_discard(slice.len());
        if swallowed == slice.len() {
            return RecvDecision::Discarded { needs_rearm };
        }
        match self.wire.process_recv(runtime, &slice[swallowed..]) {
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

    pub fn begin_discard(
        &mut self,
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
        n: u64,
    ) -> bool {
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
            Core::push_retry(driver, || Sqe::cancel(token, RECV));
        }
        true
    }

    pub fn recv_eof<C>(&mut self, more: bool) -> RecvDecision<C> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        self.core.recv_eof(more);
        self.wire.recv_eof();
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
        driver: &mut dope_core::driver::DriverContext<'_, 'd>,
        n: usize,
        ud: Token,
        idx: SlotIndex,
    ) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        self.core.send_done();
        let prepared = self.wire.after_send(Storage::new(&mut self.send, 0), n);
        let submit = self.core.submit_prepared(driver, ud, prepared);
        Self::finish_submit(&mut self.wire, submit);
        if !self.core.is_send_inflight() && self.core.take_graceful() {
            let prepared = self.wire.graceful_close(Storage::new(&mut self.send, 0));
            let submit = self.core.submit_prepared(driver, ud, prepared);
            Self::finish_submit(&mut self.wire, submit);
        }
        if self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        SendOutcome::Sent { idx, n }
    }

    pub fn send_failed(&mut self, idx: SlotIndex) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        self.core.send_done();
        self.core.mark_aborted();
        self.core.begin_close();
        SendOutcome::Close(idx)
    }
}

pub enum RecvDecision<C> {
    Drop,
    Close,
    NoChunk { needs_rearm: bool },
    Discarded { needs_rearm: bool },
    Chunk { chunk: C, needs_rearm: bool },
}
