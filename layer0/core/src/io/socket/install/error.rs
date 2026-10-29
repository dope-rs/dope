use std::num;

use dope_sys::{
    Cloexec, Errno, Nonblock,
    io::socket::{Setup, SetupOrigin, addr},
};

use crate::{
    driver::result,
    io::{
        socket,
        socket::{
            adoption,
            install::{Operation, adopt, receive, slot},
        },
    },
};

/// Exact bootstrap failure with typed validation and numeric OS causes.
#[derive(PartialEq, Eq, Debug)]
pub enum Error {
    Slot(slot::Error),
    Receive(receive::Error),
    Adopt(adopt::Error),
    FastOpen(num::NonZero<i32>),
    Address(socket::Error),
    LocalAddress(addr::Local<Errno>),
    DescriptorFlags(crate::Errno),
    CloseOnExec(crate::Errno),
    StatusFlags(crate::Errno),
    Nonblocking(crate::Errno),
    Sigpipe(crate::Errno),
    System {
        operation: Operation,
        fault: crate::Errno,
    },
    Kernel {
        operation: Operation,
        cause: result::Kernel,
    },
}

impl Error {
    pub(crate) fn reuse((reuse, errno): (socket::Reuse, dope_sys::Errno)) -> Self {
        Self::syscall(socket::reuse_step(reuse), errno)
    }

    pub(crate) fn setup(error: Setup) -> Self {
        match error.origin() {
            SetupOrigin::Cloexec(Cloexec::Get(error)) => Self::DescriptorFlags(error),
            SetupOrigin::Cloexec(Cloexec::Set(error)) => Self::CloseOnExec(error),
            SetupOrigin::Nonblock(Nonblock::Get(error)) => Self::StatusFlags(error),
            SetupOrigin::Nonblock(Nonblock::Set(error)) => Self::Nonblocking(error),
            SetupOrigin::Sigpipe(error) => Self::Sigpipe(error),
        }
    }

    pub(crate) const fn syscall(op: Operation, fault: dope_sys::Errno) -> Self {
        Self::System {
            operation: op,
            fault,
        }
    }

    pub(crate) const fn kernel(op: Operation, cause: result::Kernel) -> Self {
        Self::Kernel {
            operation: op,
            cause,
        }
    }

    pub(crate) fn validation(error: adoption::Error) -> Self {
        match error {
            adoption::Error::Syscall(errno) => Self::syscall(Operation::Validate, errno),
            adoption::Error::Role(error) => Self::Adopt(error),
        }
    }
}
