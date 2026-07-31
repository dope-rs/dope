use std::cell::Cell;
use std::mem::size_of;

const RECV_QUEUE_CAP: usize = 256;
const RECV_CAP_BYTES: usize = 1 << 20;

#[derive(Clone, Copy)]
pub(super) enum QueueState {
    Empty,
    Queued { chunks: usize, bytes: usize },
}

pub(in crate::net::port) struct RecvQueue {
    state: Cell<QueueState>,
}

const _: () = assert!(size_of::<RecvQueue>() == 24);

impl Default for RecvQueue {
    fn default() -> Self {
        Self {
            state: Cell::new(QueueState::Empty),
        }
    }
}

impl RecvQueue {
    pub(in crate::net::port) fn is_empty(&self) -> bool {
        matches!(self.state.get(), QueueState::Empty)
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
            Self::Queued { chunks, .. } => chunks,
        }
    }

    fn bytes(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Queued { bytes, .. } => bytes,
        }
    }

    pub(super) fn single(self) -> bool {
        self.chunks() == 1
    }

    pub(super) fn pushed(self, len: usize) -> Option<Self> {
        let chunks = self.chunks().checked_add(1)?;
        let bytes = self.bytes().checked_add(len)?;
        (chunks <= RECV_QUEUE_CAP && bytes <= RECV_CAP_BYTES)
            .then_some(Self::Queued { chunks, bytes })
    }

    pub(super) fn popped(self, len: usize) -> Option<Self> {
        let Self::Queued { chunks, bytes } = self else {
            return None;
        };
        if chunks == 0 || len > bytes {
            return None;
        }
        if chunks == 1 {
            return (bytes == len).then_some(Self::Empty);
        }
        Some(Self::Queued {
            chunks: chunks - 1,
            bytes: bytes - len,
        })
    }

    pub(super) fn consumed(self, amount: usize) -> Option<Self> {
        let Self::Queued { chunks, bytes } = self else {
            return None;
        };
        (amount < bytes).then_some(Self::Queued {
            chunks,
            bytes: bytes - amount,
        })
    }
}
