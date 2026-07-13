use o3::buffer::Shared;

use super::core::{Core, RecvError};
use super::egress;
use crate::transport::link::SendOutcome;
use crate::transport::wire::{RecvChunk, Vectored, Wire};
use crate::{Driver, backend};

const DEFERRED_IOV: usize = 32;

pub const PEND_EGRESS: u8 = 1;
pub const PEND_SHUTDOWN: u8 = 2;
pub const PEND_CLOSE: u8 = 4;

#[derive(Default)]
pub struct PendingFlags {
    flags: u8,
    shutdown_how: i32,
}

impl PendingFlags {
    pub fn mark(&mut self, flag: u8) -> bool {
        let was_clean = self.flags == 0;
        self.flags |= flag;
        was_clean
    }

    pub fn take_flags(&mut self) -> u8 {
        std::mem::take(&mut self.flags)
    }

    pub fn shutdown_how(&self) -> i32 {
        self.shutdown_how
    }

    pub fn set_shutdown(&mut self, how: i32) {
        self.shutdown_how = how;
    }
}

#[derive(Default)]
pub struct DeferredEgress {
    queue: egress::Queue<DEFERRED_IOV>,
    close_after: bool,
}

impl DeferredEgress {
    pub fn stage(&mut self, bytes: Shared, close: bool) -> bool {
        self.close_after |= close;
        if !self.queue.has_room(1, bytes.len()) {
            return false;
        }
        self.queue.push(bytes);
        true
    }

    pub fn is_idle(&self) -> bool {
        self.queue.total_bytes() == 0
    }

    pub fn close_after(&self) -> bool {
        self.close_after
    }

    pub fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        self.queue.prepare_send(bytes_cap)
    }

    pub fn ack(&mut self, n: usize) {
        self.queue.ack(n);
    }
}

pub struct Slot<'d, W: Wire, S> {
    pub core: Core<'d>,
    pub wire: W,
    pub state: S,
    token: backend::token::Token,
    park_idx: backend::socket::FdSlot,
}

impl<'d, W: Wire, S> Slot<'d, W, S> {
    pub fn new(
        core: Core<'d>,
        wire: W,
        token: backend::token::Token,
        park_idx: backend::socket::FdSlot,
        state: S,
    ) -> Self {
        Self {
            core,
            wire,
            state,
            token,
            park_idx,
        }
    }

    pub fn token(&self) -> backend::token::Token {
        self.token
    }

    pub fn park(&self, driver: &Driver) {
        backend::park::Parker::slot(driver, self.park_idx).wake();
    }

    pub fn make_waker(&self, driver: &Driver) -> std::task::Waker {
        backend::park::Parker::slot(driver, self.park_idx).make_waker()
    }

    pub fn flush_pending(&mut self, ud: backend::token::Token, driver: &'d Driver) {
        self.wire.flush_pending(&mut self.core, ud, driver);
    }

    pub fn seal_graceful(&mut self, ud: backend::token::Token, driver: &'d Driver) -> bool {
        if self.core.seal_graceful() {
            self.wire.on_graceful_close(&mut self.core, ud, driver);
        }
        if self.core.is_send_inflight() {
            self.core.begin_close();
            return true;
        }
        false
    }

    pub fn recv_data<'a>(&mut self, more: bool, slice: &'a [u8]) -> RecvDecision<'a> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        let needs_rearm = self.core.recv_data(more);
        let swallowed = self.core.consume_discard(slice.len());
        if swallowed == slice.len() {
            return RecvDecision::Discarded { needs_rearm };
        }
        match self.wire.process_recv(&slice[swallowed..]) {
            Some(chunk) => RecvDecision::Chunk { chunk, needs_rearm },
            None => RecvDecision::NoChunk { needs_rearm },
        }
    }

    pub fn recv_discarded(&mut self, len: u32) -> RecvDecision<'static> {
        if !self.core.is_armed() || !self.core.is_discard_armed() {
            return RecvDecision::Drop;
        }
        RecvDecision::Discarded {
            needs_rearm: self.core.recv_discarded(len),
        }
    }

    /// Drop the next `n` incoming bytes in-kernel; `false` if unsupported
    /// (caller must then discard in userspace).
    pub fn begin_discard(&mut self, n: u64, driver: &'d Driver) -> bool {
        if n == 0
            || !W::RAW_RECV
            || !self.core.kernel_discard()
            || !backend::sqe::Sqe::SUPPORTS_RECV_DISCARD
            || self.core.discard_remaining() > 0
            || self.core.is_closing()
            || self.core.close_after()
        {
            return false;
        }
        self.core.begin_discard(n);
        if self.core.is_armed() && !self.core.is_discard_armed() {
            let token = self.token;
            Core::push_retry(driver, || {
                backend::sqe::Sqe::cancel(token, backend::token::kind::RECV)
            });
        }
        true
    }

    pub fn recv_eof(&mut self, more: bool) -> RecvDecision<'static> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        self.core.recv_eof(more);
        self.wire.on_recv_eof();
        RecvDecision::Close
    }

    pub fn recv_cancelled(&mut self, more: bool) -> RecvDecision<'static> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        match self.core.recv_cancelled(more) {
            RecvError::Closed => RecvDecision::Close,
            RecvError::Live { needs_rearm } => RecvDecision::NoChunk { needs_rearm },
        }
    }

    pub fn recv_starved(&mut self, more: bool) -> RecvDecision<'static> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        RecvDecision::NoChunk {
            needs_rearm: self.core.recv_starved(more),
        }
    }

    pub fn recv_failed(&mut self, more: bool) -> RecvDecision<'static> {
        if !self.core.is_armed() {
            return RecvDecision::Drop;
        }
        self.core.recv_failed(more);
        RecvDecision::Close
    }

    pub fn send_sent(
        &mut self,
        n: usize,
        ud: backend::token::Token,
        idx: backend::token::LocalIdx,
        driver: &'d Driver,
    ) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        self.core.send_done();
        if self.wire.after_send_cqe(&mut self.core, n, ud, driver) {
            return SendOutcome::Drop;
        }
        SendOutcome::Sent { idx, n }
    }

    pub fn send_failed(&mut self, idx: backend::token::LocalIdx) -> SendOutcome {
        if !self.core.is_send_inflight() {
            return SendOutcome::Drop;
        }
        self.core.send_done();
        self.core.mark_aborted();
        self.core.begin_close();
        SendOutcome::Close(idx)
    }
}

pub enum RecvDecision<'a> {
    Drop,
    Close,
    NoChunk {
        needs_rearm: bool,
    },
    Discarded {
        needs_rearm: bool,
    },
    Chunk {
        chunk: RecvChunk<'a>,
        needs_rearm: bool,
    },
}
