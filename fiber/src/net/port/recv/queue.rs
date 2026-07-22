use std::cell::Cell;

use super::NONE;

const RECV_QUEUE_CAP: usize = 256;
const RECV_CAP_BYTES: usize = 1 << 20;

#[derive(Clone, Copy)]
pub(super) enum QueueState {
    Empty,
    Linked {
        head: u32,
        tail: u32,
        chunks: usize,
        bytes: usize,
    },
}

pub(in crate::net::port) struct RecvQueue {
    reserved: Cell<u32>,
    state: Cell<QueueState>,
}

impl Default for RecvQueue {
    fn default() -> Self {
        Self {
            reserved: Cell::new(NONE),
            state: Cell::new(QueueState::Empty),
        }
    }
}

impl RecvQueue {
    pub(in crate::net::port) fn is_empty(&self) -> bool {
        matches!(self.state.get(), QueueState::Empty)
    }

    pub(super) fn reserved(&self) -> u32 {
        self.reserved.get()
    }

    pub(super) fn set_reserved(&self, index: u32) {
        self.reserved.set(index);
    }

    pub(super) fn state(&self) -> QueueState {
        self.state.get()
    }

    pub(super) fn commit(&self, state: QueueState) {
        self.state.set(state);
    }
}

impl QueueState {
    fn chunks(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Linked { chunks, .. } => chunks,
        }
    }

    fn bytes(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Linked { bytes, .. } => bytes,
        }
    }

    pub(super) fn head(self) -> Option<u32> {
        match self {
            Self::Empty => None,
            Self::Linked { head, .. } => Some(head),
        }
    }

    pub(super) fn tail(self) -> Option<u32> {
        match self {
            Self::Empty => None,
            Self::Linked { tail, .. } => Some(tail),
        }
    }

    pub(super) fn single(self) -> Option<u32> {
        match self {
            Self::Linked {
                head,
                tail,
                chunks: 1,
                ..
            } if head == tail => Some(head),
            _ => None,
        }
    }

    pub(super) fn pushed(self, index: u32, len: usize) -> Option<Self> {
        let next = match self {
            Self::Empty => Self::Linked {
                head: index,
                tail: index,
                chunks: 1,
                bytes: len,
            },
            Self::Linked {
                head,
                chunks,
                bytes,
                ..
            } => Self::Linked {
                head,
                tail: index,
                chunks: chunks.checked_add(1)?,
                bytes: bytes.checked_add(len)?,
            },
        };
        (next.chunks() <= RECV_QUEUE_CAP && next.bytes() <= RECV_CAP_BYTES).then_some(next)
    }

    pub(super) fn popped(self, index: u32, next: u32, len: usize) -> Option<Self> {
        let Self::Linked {
            head,
            tail,
            chunks,
            bytes,
        } = self
        else {
            return None;
        };
        if head != index || len > bytes {
            return None;
        }
        if chunks == 1 {
            return (tail == index && next == NONE && bytes == len).then_some(Self::Empty);
        }
        (next != NONE).then_some(Self::Linked {
            head: next,
            tail,
            chunks: chunks - 1,
            bytes: bytes - len,
        })
    }

    pub(super) fn consumed(self, amount: usize) -> Option<Self> {
        let Self::Linked {
            head,
            tail,
            chunks,
            bytes,
        } = self
        else {
            return None;
        };
        (amount < bytes).then_some(Self::Linked {
            head,
            tail,
            chunks,
            bytes: bytes - amount,
        })
    }
}
