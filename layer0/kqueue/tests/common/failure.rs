use std::time;

use dope_sys::{
    Errno,
    io::reactor::ready,
    table::change::{self, Filter},
};

use crate::common::slots::{Slots, inputs::Missing};

type Index = <Slots as change::raw::Slots>::Index;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Timeout(time::Duration),
    Wait(Errno),
    Changes(Errno),
    Revoke {
        fd: usize,
        filter: Filter,
        errno: Errno,
    },
    Backing {
        index: Index,
        filter: Filter,
    },
    Missing(Missing),
    System(Errno),
}

impl From<ready::Cause> for Failure {
    fn from(cause: ready::Cause) -> Self {
        match cause {
            ready::Cause::Timeout(span) => Self::Timeout(span),
            ready::Cause::Wait(errno) => Self::Wait(errno),
            ready::Cause::Changes(errno) => Self::Changes(errno),
        }
    }
}

impl ready::Failure for Failure {
    type Index = Index;
    type Missing = Missing;
    type Operation = ();

    fn revoke(fd: usize, filter: Filter, errno: Errno) -> Self {
        Self::Revoke { fd, filter, errno }
    }
    fn backing(error: change::Index<Index>) -> Self {
        let (index, filter) = error.into_parts();
        Self::Backing { index, filter }
    }
    fn missing((): (), cause: Missing) -> Self {
        Self::Missing(cause)
    }
    fn system((): (), errno: Errno) -> Self {
        Self::System(errno)
    }
}
