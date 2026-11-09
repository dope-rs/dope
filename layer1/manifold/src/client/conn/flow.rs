use std::mem;

use dope_core::driver::route::Identity;

use crate::client::conn::{claim, drain};

#[must_use = "callback flow must handle discarded transport"]
pub(in crate::client) enum Flow<T> {
    Continue(T),
    Discarded(T),
}

#[must_use = "active callback admission must preserve opening and sealed state"]
pub(in crate::client) enum Active<T> {
    Ready(Flow<T>),
    Opening,
    Sealed,
}

#[must_use = "close callback must preserve settlement and sealed state"]
pub(super) enum Close<'d, ID: Identity> {
    Complete(drain::Drain<'d, ID>, claim::Urgency),
    Yield,
    Blocked,
    Sealed,
}

#[must_use = "close application must handle a sealed connection"]
pub(in crate::client) enum Apply {
    Applied(Flow<()>),
    Sealed,
}

impl<T> Flow<T> {
    pub(in crate::client) fn into_parts(self) -> (T, Flow<()>) {
        match self {
            Self::Continue(output) => (output, Flow::Continue(())),
            Self::Discarded(output) => (output, Flow::Discarded(())),
        }
    }
}

const _: () = assert!(mem::size_of::<Active<()>>() == mem::size_of::<Flow<()>>());
