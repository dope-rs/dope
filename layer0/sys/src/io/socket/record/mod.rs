use crate::Errno;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum End {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Open(Errno),
    Cloexec { end: End, error: crate::Cloexec },
    Sigpipe { end: End, error: Errno },
}
