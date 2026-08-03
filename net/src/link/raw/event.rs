use dope_core::driver::token::{SlotIndex, Token};

pub struct SendCompletion {
    target: Token,
    bytes: usize,
}

impl SendCompletion {
    pub(in crate::link) fn new(target: Token, bytes: usize) -> Self {
        Self { target, bytes }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::link) fn target(&self) -> Token {
        self.target
    }
}

pub enum SocketStep<X> {
    Connecting,
    Failed { peeked: Option<X> },
}

pub enum ConnectStep<X> {
    Connected { idx: SlotIndex, peeked: X },
    Failed { peeked: X },
    Drop { peeked: Option<X> },
}

pub enum DispatchRecv<C> {
    Drop,
    Close(SlotIndex),
    Chunk(SlotIndex, C),
    NoChunk(SlotIndex),
    Discarded(SlotIndex),
}

pub enum SendOutcome {
    Sent {
        idx: SlotIndex,
        completion: SendCompletion,
    },
    Close {
        idx: SlotIndex,
        completion: SendCompletion,
    },
    Drop,
}
