use std::cell::Cell;
use std::io::{Error, ErrorKind};

use dope::driver::ready::{CompletionSlot, CompletionWaker};
use dope_net::wire::{RecvCursor, RecvTarget};
use o3::cell::RegionToken;

use super::recv::arena::{PushError, RecvArena};
use super::recv::queue::RecvQueue;
use super::result::{RecvInto, SendIdle};

pub(crate) struct State<'d> {
    recv: RecvQueue,
    closed: Cell<bool>,
    error: Cell<Option<Error>>,
    recv_waiter: CompletionSlot<'d>,
    send_waiter: CompletionSlot<'d>,
    detached: Cell<bool>,
}

impl Default for State<'_> {
    fn default() -> Self {
        Self {
            recv: RecvQueue::default(),
            closed: Cell::new(false),
            error: Cell::new(None),
            recv_waiter: CompletionSlot::empty(),
            send_waiter: CompletionSlot::empty(),
            detached: Cell::new(false),
        }
    }
}

impl<'d> State<'d> {
    pub(crate) fn reset<R: RecvCursor + 'd>(
        &self,
        lane: usize,
        arena: &RecvArena<'d, R>,
        region: &mut RegionToken<'d>,
    ) {
        arena.reset(lane, &self.recv, region);
        self.closed.set(false);
        self.error.take();
        self.recv_waiter.clear();
        self.send_waiter.clear();
        self.detached.set(false);
    }

    pub(crate) fn push_retained<R: RecvCursor + 'd>(
        &self,
        lane: usize,
        arena: &RecvArena<'d, R>,
        chunk: R,
        region: &mut RegionToken<'d>,
    ) -> bool {
        let len = chunk.remaining();
        if len == 0 {
            return false;
        }
        if self.is_closed() {
            return true;
        }
        if let Err(error) = arena.push(lane, &self.recv, chunk, region) {
            let message = match error {
                PushError::Limit => "fiber: recv backpressure exceeded",
                PushError::Exhausted => "fiber: receive arena exhausted",
            };
            self.signal_error(Error::new(ErrorKind::OutOfMemory, message));
            return true;
        }
        Self::wake(&self.recv_waiter);
        false
    }

    pub(crate) fn wake_send(&self) {
        Self::wake(&self.send_waiter);
    }

    pub(crate) fn signal_error(&self, e: Error) {
        self.error.set(Some(e));
        self.closed.set(true);
        Self::wake(&self.recv_waiter);
        Self::wake(&self.send_waiter);
    }

    pub(crate) fn signal_closed(&self) {
        self.closed.set(true);
        Self::wake(&self.recv_waiter);
        Self::wake(&self.send_waiter);
    }

    fn is_closed(&self) -> bool {
        self.closed.get()
    }

    fn take_error(&self) -> Option<Error> {
        self.error.take()
    }

    fn wake(waiter: &CompletionSlot<'d>) {
        if let Some(wake) = waiter.take() {
            wake.wake();
        }
    }

    pub(crate) fn set_recv_waker(&self, wake: CompletionWaker<'d>) {
        self.recv_waiter.set(wake);
    }

    pub(crate) fn clear_recv_waker(&self) {
        self.recv_waiter.clear();
    }

    pub(crate) fn set_send_waker(&self, wake: CompletionWaker<'d>) {
        self.send_waiter.set(wake);
    }

    pub(crate) fn clear_send_waker(&self) {
        self.send_waiter.clear();
    }

    pub(crate) fn detach(&self) {
        self.detached.set(true);
    }

    pub(crate) fn readable_drained(&self) -> bool {
        self.detached.get() || self.recv.is_empty()
    }

    pub(crate) fn try_recv_into<R: RecvCursor + 'd>(
        &self,
        lane: usize,
        arena: &RecvArena<'d, R>,
        target: &mut RecvTarget<'_>,
        region: &mut RegionToken<'d>,
    ) -> RecvInto {
        let initial = target.len();
        arena.drain_into(lane, &self.recv, target, region);
        if target.len() != initial {
            return RecvInto::Ready;
        }
        if let Some(e) = self.take_error() {
            return RecvInto::Failed(e);
        }
        if self.is_closed() {
            return RecvInto::Ready;
        }
        RecvInto::Pending
    }

    pub(crate) fn send_status(&self, inflight: bool) -> SendIdle {
        if let Some(e) = self.take_error() {
            return SendIdle::Failed(e);
        }
        if !inflight {
            return SendIdle::Idle;
        }
        if self.is_closed() {
            return SendIdle::Failed(Error::new(ErrorKind::BrokenPipe, "fiber: closed"));
        }
        SendIdle::Pending
    }
}
