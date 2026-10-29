use std::mem;

use crate::client::app;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Effect {
    Preserved,
    Discarded,
}

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
pub(in crate::client) enum Close {
    Ready(Flow<app::CloseOutcome>),
    Sealed,
}

#[must_use = "close application must handle a sealed connection"]
pub(in crate::client) enum Apply {
    Applied(Flow<()>),
    Sealed,
}

#[derive(Clone, Copy)]
pub(in crate::client) enum Follow {
    Unbound,
    Bound,
    Released,
}

impl<T> Flow<T> {
    pub(super) fn from_effect(output: T, effect: Effect) -> Self {
        match effect {
            Effect::Preserved => Self::Continue(output),
            Effect::Discarded => Self::Discarded(output),
        }
    }

    pub(in crate::client) fn into_parts(self) -> (T, Flow<()>) {
        match self {
            Self::Continue(output) => (output, Flow::Continue(())),
            Self::Discarded(output) => (output, Flow::Discarded(())),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Active<()>>() == mem::size_of::<Flow<()>>());
    assert!(mem::size_of::<Close>() == mem::size_of::<Flow<app::CloseOutcome>>());
};
