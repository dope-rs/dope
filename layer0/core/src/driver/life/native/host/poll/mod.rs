pub(crate) mod events;
pub(in crate::driver::life::native::host) mod file;
pub(in crate::driver::life::native::host) mod files;
pub(in crate::driver::life::native::host) mod raw;

use std::{mem, time};

use dope_sys::{
    io::reactor::ready,
    table::{change, change::Filter},
};

use crate::driver::{
    result::{poll, poll::Operation},
    route::indices,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Failure {
    Timeout {
        requested: time::Duration,
    },
    System {
        operation: Operation,
        fault: dope_sys::Errno,
    },
    Revoke {
        descriptor: usize,
        filter: Filter,
        errno: dope_sys::Errno,
    },
    Backing {
        operation: Operation,
        index: indices::SlotIndex,
        filter: Filter,
    },
    Missing {
        operation: Operation,
        cause: poll::Missing,
    },
}

impl From<ready::Cause> for Failure {
    fn from(cause: ready::Cause) -> Self {
        match cause {
            ready::Cause::Timeout(span) => Self::Timeout { requested: span },
            ready::Cause::Wait(errno) => Self::System {
                operation: Operation::Wait,
                fault: errno,
            },
            ready::Cause::Changes(errno) => Self::System {
                operation: Operation::Changes,
                fault: errno,
            },
        }
    }
}

impl ready::Failure for Failure {
    type Index = indices::SlotIndex;
    type Missing = poll::Missing;
    type Operation = Operation;

    fn revoke(fd: usize, filter: Filter, errno: dope_sys::Errno) -> Self {
        Self::Revoke {
            descriptor: fd,
            filter,
            errno,
        }
    }

    fn backing(error: change::Index<Self::Index>) -> Self {
        let (index, filter) = error.into_parts();
        Self::Backing {
            operation: Operation::Changes,
            index,
            filter,
        }
    }

    fn missing(op: Operation, cause: poll::Missing) -> Self {
        Self::Missing {
            operation: op,
            cause,
        }
    }

    fn system(op: Operation, errno: dope_sys::Errno) -> Self {
        Self::System {
            operation: op,
            fault: errno,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Failure>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Result<(), Failure>>() == mem::size_of::<Failure>());
};

impl From<Failure> for poll::Cause {
    fn from(error: Failure) -> Self {
        match error {
            Failure::Timeout { requested: span } => Self::Timeout { requested: span },
            Failure::System {
                operation: op,
                fault,
            } => Self::System {
                operation: op,
                fault,
            },
            Failure::Revoke {
                descriptor: fd,
                filter,
                errno,
            } => Self::Revoke {
                descriptor: fd,
                filter: poll::Filter::from_native(filter),
                errno,
            },
            Failure::Backing {
                operation: op,
                index,
                filter,
            } => Self::Backing {
                operation: op,
                index,
                filter: poll::Filter::from_native(filter),
            },
            Failure::Missing {
                operation: op,
                cause,
            } => Self::Missing {
                operation: op,
                cause,
            },
        }
    }
}
