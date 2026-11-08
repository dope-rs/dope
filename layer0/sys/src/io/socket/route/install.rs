use crate::Errno;

/// Exact failure while installing a socket-routing program.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Error {
    pub operation: Operation,
    pub errno: Errno,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    ProgramAttach,
    FilterAttach,
}
