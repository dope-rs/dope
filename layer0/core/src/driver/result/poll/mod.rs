use std::{mem, os::fd, task};

use dope_sys::table::change;

use crate::driver::route::{indices, table::bounds};

mod cause;
mod dispatch;
mod error;
pub mod fault;

pub use cause::Cause;
pub use dispatch::Dispatch;
pub use error::Error;

/// Exact reactor operation that failed on a readiness-queue backend.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Operation {
    Wait,
    Changes,
    Accept,
    Connect,
    SocketError,
    Recv,
    RecvMsg,
    Send,
    Shutdown,
    FileOpen,
    FileWrite,
    FileSync,
    FileRename,
    FileUnlink,
}

/// Exact descriptor-table absence observed by a readiness backend.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum Missing {
    Bounds(bounds::Excluded),
    Backing {
        index: indices::SlotIndex,
    },
    Vacant {
        index: indices::SlotIndex,
    },
    Replaced {
        index: indices::SlotIndex,
        expected: fd::RawFd,
        actual: fd::RawFd,
    },
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[must_use = "pending backend events must be drained before blocking"]
pub enum Drain {
    /// The backend's visible event queue was empty after synchronization.
    Drained,
    /// Visible events remain and require another reactor turn.
    Pending,
}

impl From<task::Poll<()>> for Drain {
    fn from(state: task::Poll<()>) -> Self {
        match state {
            task::Poll::Ready(()) => Self::Drained,
            task::Poll::Pending => Self::Pending,
        }
    }
}

/// Backend publication progress, not completion of submitted I/O.
#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use = "pending backend changes must be committed in a later reactor turn"]
pub enum Commit {
    /// Every change queued before this call has reached the backend.
    Drained,
    /// Queued changes remain and require another reactor turn.
    Pending,
}

const _: () = {
    assert!(mem::size_of::<Missing>() == 4 * mem::size_of::<u32>());
    assert!(mem::align_of::<Missing>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Operation>() == 1);
    assert!(core::mem::size_of::<Result<(), Error>>() == core::mem::size_of::<Error>());
    assert!(core::mem::size_of::<Result<Commit, Error>>() == core::mem::size_of::<Error>());
};

/// Descriptor retirement distinguishes missing ownership from the exact close result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Close {
    Missing(Missing),
    Closed(Result<(), crate::Errno>),
}

/// Descriptor readiness direction whose native change could not be committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Filter {
    Read,
    Write,
}

impl Filter {
    pub(crate) const fn from_native(filter: change::Filter) -> Self {
        use dope_sys::table::change::Filter;
        match filter {
            Filter::Read => Self::Read,
            Filter::Write => Self::Write,
        }
    }
}
