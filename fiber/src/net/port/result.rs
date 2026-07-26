use std::io::Error;

pub(crate) enum RecvInto {
    Bytes(usize),
    Failed(Error),
    Pending,
}

pub(crate) enum SendIdle {
    Idle,
    Failed(Error),
    Pending,
}
