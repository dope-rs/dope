use std::cell::Cell;
use std::io::{self, Error, ErrorKind};

use crate::Waker;
use crate::io::RecvBuffer;
use dope::io::provided::ProvidedView;
use o3::buffer::RetainBytes;

use super::recv::arena::{PushError, RecvArena};
use super::recv::queue::RecvQueue;
use super::result::{RecvInto, SendIdle};

pub(crate) struct State<'d> {
    recv: RecvQueue,
    closed: Cell<bool>,
    error: Cell<Option<io::Error>>,
    recv_waiter: Cell<Option<Waker<'d>>>,
    send_waiter: Cell<Option<Waker<'d>>>,
    detached: Cell<bool>,
}

impl Default for State<'_> {
    fn default() -> Self {
        Self {
            recv: RecvQueue::default(),
            closed: Cell::new(false),
            error: Cell::new(None),
            recv_waiter: Cell::new(None),
            send_waiter: Cell::new(None),
            detached: Cell::new(false),
        }
    }
}

impl<'d> State<'d> {
    pub(crate) fn reset(&self, arena: &RecvArena<'d>) -> bool {
        let reserved = arena.reset(&self.recv);
        self.closed.set(false);
        self.error.take();
        self.recv_waiter.set(None);
        self.send_waiter.set(None);
        self.detached.set(false);
        reserved
    }

    pub(crate) fn push_recv<R: RetainBytes>(&self, arena: &RecvArena<'d>, chunk: R) -> bool {
        let len = chunk.len();
        self.push_recv_value(arena, len, || RecvBuffer::Owned(chunk.into_retained()))
    }

    pub(crate) fn push_retained(&self, arena: &RecvArena<'d>, chunk: ProvidedView<'d>) -> bool {
        let len = chunk.len();
        self.push_recv_value(arena, len, || RecvBuffer::Provided(chunk))
    }

    fn push_recv_value(
        &self,
        arena: &RecvArena<'d>,
        len: usize,
        value: impl FnOnce() -> RecvBuffer<'d>,
    ) -> bool {
        if len == 0 {
            return false;
        }
        if self.is_closed() {
            return true;
        }
        if let Err(error) = arena.push(&self.recv, value(), len as u32) {
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

    pub(crate) fn signal_error(&self, e: io::Error) {
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

    fn take_error(&self) -> Option<io::Error> {
        self.error.take()
    }

    fn wake(waiter: &Cell<Option<Waker<'d>>>) {
        if let Some(waker) = waiter.take() {
            waker.wake();
        }
    }

    pub(crate) fn set_recv_waker(&self, waker: Waker<'d>) {
        self.recv_waiter.set(Some(waker));
    }

    pub(crate) fn clear_recv_waker(&self) {
        self.recv_waiter.set(None);
    }

    pub(crate) fn set_send_waker(&self, waker: Waker<'d>) {
        self.send_waiter.set(Some(waker));
    }

    pub(crate) fn clear_send_waker(&self) {
        self.send_waiter.set(None);
    }

    pub(crate) fn detach(&self) {
        self.detached.set(true);
    }

    pub(crate) fn readable_drained(&self) -> bool {
        self.detached.get() || self.recv.is_empty()
    }

    pub(crate) fn try_recv_into(&self, arena: &RecvArena<'d>, dst: &mut [u8]) -> RecvInto {
        let filled = arena.drain_into(&self.recv, dst);
        if filled > 0 {
            return RecvInto::Bytes(filled);
        }
        if let Some(e) = self.take_error() {
            return RecvInto::Failed(e);
        }
        if self.is_closed() {
            return RecvInto::Bytes(0);
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
