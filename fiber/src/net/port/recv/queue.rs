use std::{cell, mem};

const RECV_BYTE_CAP: u32 = 1 << 20;

#[derive(Clone, Copy, Default)]
#[repr(transparent)]
struct BufferedBytes(u32);

pub(in crate::net::port) struct RecvQueue {
    bytes: cell::Cell<BufferedBytes>,
}

#[must_use = "a prepared receive queue update has no effect until committed"]
pub(super) struct Update<'queue> {
    queue: &'queue RecvQueue,
    next: BufferedBytes,
}

const _: () = {
    assert!(mem::size_of::<BufferedBytes>() == mem::size_of::<u32>());
    assert!(mem::align_of::<BufferedBytes>() == mem::align_of::<u32>());
    assert!(mem::size_of::<RecvQueue>() == mem::size_of::<u32>());
};

impl Default for RecvQueue {
    fn default() -> Self {
        Self {
            bytes: cell::Cell::new(BufferedBytes::default()),
        }
    }
}

impl RecvQueue {
    pub(in crate::net::port) fn is_empty(&self) -> bool {
        self.bytes.get().is_empty()
    }

    pub(super) fn prepare_push(&self, len: u32) -> Option<Update<'_>> {
        Some(self.prepare(self.bytes.get().pushed(len)?))
    }

    pub(super) fn prepare_pop(&self, len: u32) -> Option<Update<'_>> {
        Some(self.prepare(self.bytes.get().popped(len)?))
    }

    fn prepare(&self, next: BufferedBytes) -> Update<'_> {
        Update { queue: self, next }
    }
}

impl BufferedBytes {
    fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn pushed(self, len: u32) -> Option<Self> {
        if len == 0 {
            return None;
        }
        let next = self.0.checked_add(len)?;
        (next <= RECV_BYTE_CAP).then_some(Self(next))
    }

    fn popped(self, len: u32) -> Option<Self> {
        if len == 0 {
            return None;
        }
        self.0.checked_sub(len).map(Self)
    }
}

impl Update<'_> {
    pub(super) fn commit(self) {
        self.queue.bytes.set(self.next);
    }

    pub(super) fn leaves_empty(&self) -> bool {
        self.next.is_empty()
    }
}
