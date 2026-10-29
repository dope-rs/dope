use std::mem;

use dope_core::{driver::schedule, io::Event};

mod idle;
pub(super) mod modes;
mod parked;
mod polled;
pub(crate) mod ready;
pub(crate) mod routes;
pub(crate) mod scope;

pub(crate) use idle::Idle;
pub(crate) use parked::{Parked, Status};
pub(crate) use polled::Polled;

const _: () = {
    assert!(mem::size_of::<Option<Event<'static>>>() == mem::size_of::<Event<'static>>());
    assert!(mem::align_of::<Option<Event<'static>>>() == mem::align_of::<Event<'static>>());
};

#[must_use]
pub(crate) enum Sync<T> {
    Again,
    Idle(T),
}

/// Resolution of one idle turn supplied to [`Parked::resolve`].
pub(crate) enum Resolved<'turn, 'd, M> {
    /// Re-poll the current turn without blocking.
    Again,
    /// Block the driver until `progress` next becomes runnable.
    Wait(Idle<'turn, 'd, M>, schedule::Progress<'d>),
    /// The caller's loop is resolved.
    Done,
}
