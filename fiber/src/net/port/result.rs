use std::io::Error;

pub(crate) enum RecvInto {
    Ready,
    Failed(Error),
    Pending,
}

pub(crate) enum SendIdle {
    Idle,
    Failed(Error),
    Pending,
}
