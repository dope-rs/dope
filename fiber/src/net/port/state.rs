use std::cell::Cell;
use std::io::{self, Error, ErrorKind};

use crate::Waker;
use crate::io::RecvBuffer;
use dope::ProvidedView;
use o3::buffer::RetainBytes;

use super::recv::RecvSlot;

const RECV_QUEUE_CAP: usize = 256;
const RECV_CAP_BYTES: usize = 1 << 20;
const RECV_SLOTS_PER_CONN: usize = 4;
const NONE: u32 = u32::MAX;

pub(crate) struct RecvArena<'d> {
    slots: Box<[RecvSlot<'d>]>,
    reserved_free: Cell<u32>,
    shared_free: Cell<u32>,
}

impl<'d> RecvArena<'d> {
    pub(crate) fn capacity_for(connections: usize) -> usize {
        connections
            .checked_mul(RECV_SLOTS_PER_CONN)
            .expect("fiber: connection capacity out of range")
            .max(RECV_QUEUE_CAP)
    }

    pub(crate) fn for_connections(connections: usize) -> Self {
        assert!(
            connections > 0 && connections <= (NONE as usize - 1) / RECV_SLOTS_PER_CONN,
            "fiber: connection capacity out of range"
        );
        let capacity = Self::capacity_for(connections);
        let slots: Box<[_]> = (0..capacity)
            .map(|index| {
                RecvSlot::new(if index + 1 == connections || index + 1 == capacity {
                    NONE
                } else {
                    (index + 1) as u32
                })
            })
            .collect();
        Self {
            reserved_free: Cell::new(0),
            shared_free: Cell::new(connections as u32),
            slots,
        }
    }

    fn reserve(&self, state: &State<'d>) {
        if state.recv_reserved.get() != NONE {
            return;
        }
        let index = self.reserved_free.get();
        assert!(index != NONE, "fiber: receive reservation exhausted");
        let slot = &self.slots[index as usize];
        self.reserved_free.set(slot.next());
        slot.set_next(NONE);
        state.recv_reserved.set(index);
    }

    fn push(
        &self,
        state: &State<'d>,
        value: RecvBuffer<'d>,
        len: u32,
    ) -> Result<(), RecvBuffer<'d>> {
        let reserved = state.recv_reserved.get();
        assert!(reserved != NONE, "fiber: missing receive reservation");
        let reserved_slot = &self.slots[reserved as usize];
        if state.recv_head.get() == NONE {
            debug_assert_eq!(state.recv_tail.get(), NONE);
            debug_assert_eq!(state.recv_len.get(), 0);
            debug_assert!(reserved_slot.is_empty());
            reserved_slot.insert(value, len);
            state.recv_head.set(reserved);
            state.recv_tail.set(reserved);
            state.recv_len.set(1);
            return Ok(());
        }
        let index = if reserved_slot.is_empty() {
            reserved
        } else {
            let index = self.shared_free.get();
            if index == NONE {
                return Err(value);
            }
            self.shared_free.set(self.slots[index as usize].next());
            index
        };
        let slot = &self.slots[index as usize];
        slot.set_next(NONE);
        slot.insert(value, len);
        let tail = state.recv_tail.replace(index);
        if tail == NONE {
            state.recv_head.set(index);
        } else {
            self.slots[tail as usize].set_next(index);
        }
        state.recv_len.set(state.recv_len.get() + 1);
        Ok(())
    }

    fn pop(&self, state: &State<'d>) -> Option<RecvBuffer<'d>> {
        let index = state.recv_head.get();
        if index == NONE {
            return None;
        }
        let slot = &self.slots[index as usize];
        let next = slot.next();
        state.recv_head.set(next);
        if next == NONE {
            state.recv_tail.set(NONE);
        }
        state.recv_len.set(state.recv_len.get() - 1);
        let value = slot.take().unwrap();
        if index == state.recv_reserved.get() {
            slot.set_next(NONE);
        } else {
            slot.set_next(self.shared_free.replace(index));
        }
        Some(value)
    }

    fn reset(&self, state: &State<'d>) {
        while self.pop(state).is_some() {}
        self.reserve(state);
    }
}

pub enum RecvInto {
    Bytes(usize),
    Failed(io::Error),
    Pending,
}

pub enum RecvChunkResult<'d> {
    Chunk(RecvBuffer<'d>),
    Failed(io::Error),
    Closed,
    Pending,
}

pub enum SendIdle {
    Idle,
    Failed(io::Error),
    Pending,
}

pub(crate) struct State<'d> {
    recv_reserved: Cell<u32>,
    recv_head: Cell<u32>,
    recv_tail: Cell<u32>,
    recv_len: Cell<usize>,
    recv_queued_bytes: Cell<usize>,
    closed: Cell<bool>,
    error: Cell<Option<io::Error>>,
    recv_waiter: Cell<Option<Waker<'d>>>,
    send_waiter: Cell<Option<Waker<'d>>>,
    detached: Cell<bool>,
}

impl Default for State<'_> {
    fn default() -> Self {
        Self {
            recv_reserved: Cell::new(NONE),
            recv_head: Cell::new(NONE),
            recv_tail: Cell::new(NONE),
            recv_len: Cell::new(0),
            recv_queued_bytes: Cell::new(0),
            closed: Cell::new(false),
            error: Cell::new(None),
            recv_waiter: Cell::new(None),
            send_waiter: Cell::new(None),
            detached: Cell::new(false),
        }
    }
}

impl<'d> State<'d> {
    pub(crate) fn reset(&self, arena: &RecvArena<'d>) {
        arena.reset(self);
        self.recv_queued_bytes.set(0);
        self.closed.set(false);
        self.error.take();
        self.recv_waiter.set(None);
        self.send_waiter.set(None);
        self.detached.set(false);
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
        let queued = self.recv_queued_bytes.get();
        let chunks = self.recv_len.get();
        if len > RECV_CAP_BYTES - queued || chunks == RECV_QUEUE_CAP {
            self.signal_error(Error::new(
                ErrorKind::OutOfMemory,
                "fiber: recv backpressure exceeded",
            ));
            return true;
        }
        if arena.push(self, value(), len as u32).is_err() {
            self.signal_error(Error::new(
                ErrorKind::OutOfMemory,
                "fiber: receive arena exhausted",
            ));
            return true;
        }
        self.recv_queued_bytes.set(queued + len);
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
        if self.detached.get() {
            return true;
        }
        self.recv_head.get() == NONE
    }

    pub(crate) fn try_recv_into(&self, arena: &RecvArena<'d>, dst: &mut [u8]) -> RecvInto {
        let filled = self.drain_into(arena, dst);
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

    pub(crate) fn try_recv_chunk(&self, arena: &RecvArena<'d>) -> RecvChunkResult<'d> {
        if let Some(chunk) = arena.pop(self) {
            let len = chunk.len();
            let queued = self.recv_queued_bytes.get();
            debug_assert!(queued >= len);
            self.recv_queued_bytes.set(queued - len);
            return RecvChunkResult::Chunk(chunk);
        }
        if let Some(error) = self.take_error() {
            return RecvChunkResult::Failed(error);
        }
        if self.is_closed() {
            return RecvChunkResult::Closed;
        }
        RecvChunkResult::Pending
    }

    fn drain_into(&self, arena: &RecvArena<'d>, dst: &mut [u8]) -> usize {
        let head = self.recv_head.get();
        if head != NONE && head == self.recv_reserved.get() && self.recv_tail.get() == head {
            let slot = &arena.slots[head as usize];
            let len = slot.len();
            if len <= dst.len() {
                slot.copy_prefix(&mut dst[..len]);
                self.recv_head.set(NONE);
                self.recv_tail.set(NONE);
                self.recv_len.set(0);
                let queued = self.recv_queued_bytes.get();
                debug_assert!(queued >= len);
                self.recv_queued_bytes.set(queued - len);
                drop(slot.take().unwrap());
                return len;
            }
        }

        let mut written = 0usize;
        while written < dst.len() {
            let index = self.recv_head.get();
            if index == NONE {
                break;
            }
            let slot = &arena.slots[index as usize];
            let len = slot.len();
            let want = (dst.len() - written).min(len);
            slot.copy_prefix(&mut dst[written..written + want]);
            written += want;
            self.recv_queued_bytes
                .set(self.recv_queued_bytes.get().saturating_sub(want));
            if want < len {
                slot.advance(want);
            } else {
                drop(arena.pop(self));
            }
        }
        written
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
