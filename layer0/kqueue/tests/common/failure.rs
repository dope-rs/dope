use dope_sys::{
    Errno,
    io::reactor::ready,
    table::change::{self, Filter},
};

use crate::common::slots::{Slots, inputs::Missing};

type Index = <Slots as change::raw::Slots>::Index;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Ready(ready::Cause),
    Revoke {
        fd: usize,
        filter: Filter,
        errno: Errno,
    },
    Backing(change::Index<Index>),
    Missing(Missing),
    System(Errno),
}

impl From<ready::Cause> for Failure {
    fn from(cause: ready::Cause) -> Self {
        Self::Ready(cause)
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
        Self::Backing(error)
    }
    fn missing((): (), cause: Missing) -> Self {
        Self::Missing(cause)
    }
    fn system((): (), errno: Errno) -> Self {
        Self::System(errno)
    }
}
