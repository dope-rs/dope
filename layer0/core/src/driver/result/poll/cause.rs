use std::time;

use dope_sys::ring::enter;

use crate::driver::{
    life::native::create::Kernel,
    result,
    result::poll::{Filter, Missing, Operation},
    route::indices,
};

/// Lossless, allocation-free reactor polling failure.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Cause {
    Timeout {
        requested: time::Duration,
    },
    Submit {
        cause: result::Kernel,
    },
    Wait {
        cause: result::Kernel,
    },
    Cancel {
        cause: result::Kernel,
    },
    System {
        operation: Operation,
        fault: crate::Errno,
    },
    Revoke {
        descriptor: usize,
        filter: Filter,
        errno: crate::Errno,
    },
    Backing {
        operation: Operation,
        index: indices::SlotIndex,
        filter: Filter,
    },
    Missing {
        operation: Operation,
        cause: Missing,
    },
}

impl<E, C> From<enter::Poll<E, C>> for Cause
where
    Kernel: From<E> + From<C>,
{
    fn from(error: enter::Poll<E, C>) -> Self {
        match error {
            enter::Poll::Timeout { requested: span } => Self::Timeout { requested: span },
            enter::Poll::Submit(cause) => Self::Submit {
                cause: Kernel::from(cause).0,
            },
            enter::Poll::Wait(cause) => Self::Wait {
                cause: Kernel::from(cause).0,
            },
            enter::Poll::Cancel(cause) => Self::Cancel {
                cause: Kernel::from(cause).0,
            },
        }
    }
}
