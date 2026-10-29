use crate::Errno;

/// Exact failure while installing a socket-routing program.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Error {
    pub operation: Operation,
    pub errno: Errno,
}

/// Exact routing preparation failure, including absent native support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Unsupported(super::Plan),
    Syscall(Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    MapCreate,
    ProgramLoad,
    ProgramAttach,
    MapInsert,
}
