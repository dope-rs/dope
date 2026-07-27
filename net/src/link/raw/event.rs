use dope_core::driver::token::SlotIndex;

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
    Sent { idx: SlotIndex, n: usize },
    Close(SlotIndex),
    Drop,
}
