use std::mem::size_of;

use dope_sys::io::reactor::complete::flags;

/// Whether a multishot operation remains armed after this completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Flow {
    Last,
    More,
}

impl From<flags::Flow> for Flow {
    fn from(flow: flags::Flow) -> Self {
        match flow {
            flags::Flow::Last => Self::Last,
            flags::Flow::More => Self::More,
        }
    }
}

pub(super) struct Item<T> {
    flow: Flow,
    value: T,
}

impl<T> Item<T> {
    pub(super) const fn value(&self) -> &T {
        &self.value
    }

    pub(super) const fn flow(&self) -> Flow {
        self.flow
    }

    pub(super) const fn new(flow: Flow, value: T) -> Self {
        Self { flow, value }
    }

    pub(super) fn into_parts(self) -> (Flow, T) {
        (self.flow, self.value)
    }
}

const _: () = assert!(size_of::<Flow>() == size_of::<bool>());
