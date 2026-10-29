use std::net;

use dope_sys::io::socket::{addr, route, route::install};
use o3::alloc;

use crate::{
    Errno,
    io::{datagram, socket, socket::Create},
};

/// Exact routed-group construction failure.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Allocation(alloc::Error),
    Unsupported {
        address: net::SocketAddr,
        plan: route::Plan,
    },
    Address(socket::Error),
    LocalAddress(addr::Local<dope_sys::Errno>),
    Create(Create),
    Syscall {
        operation: datagram::Operation,
        errno: Errno,
    },
}

impl From<(socket::Reuse, Errno)> for Error {
    fn from((reuse, errno): (socket::Reuse, Errno)) -> Self {
        Self::Syscall {
            operation: socket::reuse_step(reuse),
            errno,
        }
    }
}

impl Error {
    pub(crate) fn install(error: install::Error) -> Self {
        let op = match error.operation {
            install::Operation::MapCreate => datagram::Operation::MapCreate,
            install::Operation::ProgramLoad => datagram::Operation::ProgramLoad,
            install::Operation::ProgramAttach => datagram::Operation::ProgramAttach,
            install::Operation::MapInsert => datagram::Operation::MapInsert,
        };
        Self::Syscall {
            operation: op,
            errno: error.errno,
        }
    }
}
