use std::io;

use crate::io::RecvBuffer;

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
