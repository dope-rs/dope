use std::net::SocketAddr;
use std::ops::Range;

pub enum RecvOutcome {
    Packet {
        src: SocketAddr,
        payload: Range<usize>,
    },
    Truncated {
        src: SocketAddr,
        partial: Range<usize>,
    },
    Empty,
    Error(i32),
}
