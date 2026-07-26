use std::io::Error;

pub enum RecvInto {
    Bytes(usize),
    Failed(Error),
    Pending,
}

pub enum SendIdle {
    Idle,
    Failed(Error),
    Pending,
}
