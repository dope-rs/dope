use std::io;

pub enum RecvInto {
    Bytes(usize),
    Failed(io::Error),
    Pending,
}

pub enum SendIdle {
    Idle,
    Failed(io::Error),
    Pending,
}
