pub(crate) mod buffers;
pub(crate) mod cq;
pub(crate) mod mappings;
pub(crate) mod op;
pub(crate) mod register;
pub(crate) mod ring;
pub(crate) mod sq;
pub(crate) mod types;

use dope_sys::{
    io::{Close, Retry},
    probe::session,
    ring::{map, register::Occupied as _},
};
use o3::mem::Rejected;

/// Retains a payload until its native owner closes successfully.
pub struct Held<O, K> {
    owner: O,
    keep: K,
}

impl<O, K> Held<O, K> {
    pub(crate) const fn new(owner: O, keep: K) -> Self {
        Self { owner, keep }
    }
}

impl<O: Close, K> Close for Held<O, K> {
    type State = Held<O::State, K>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { owner, keep } = self;
        owner.try_close().map_err(|owner| Held { owner, keep })
    }
}

impl<S: Retry, K> Retry for Held<S, K> {
    type Failure = S::Failure;
    type Owner = Held<S::Owner, K>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        let Self { owner, keep } = self;
        owner.recover().map(|owner| Held { owner, keep })
    }
}

impl<S: map::Unmap, K> map::Unmap for Held<S, K> {
    type Error = S::Error;

    fn failure(&self) -> map::Storage<Self::Error> {
        self.owner.failure()
    }
}

impl<S: map::Unmap> session::Closed for Held<S, register::raw::Occupied> {
    type Target = sq::Fixed;

    fn slot(&self) -> Self::Target {
        self.keep.slot()
    }
}
