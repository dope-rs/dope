pub(crate) mod events;
pub(in crate::driver::life::native::host) mod file;
pub(in crate::driver::life::native::host) mod files;
pub(in crate::driver::life::native::host) mod raw;

use std::mem;

use dope_sys::{
    io::reactor::ready,
    table::{change, change::Filter},
};

use crate::driver::{
    result::{poll, poll::Operation},
    route::indices,
};

pub(crate) type Failure = poll::Cause;

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
