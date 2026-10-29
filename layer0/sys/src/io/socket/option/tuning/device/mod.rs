pub mod raw;

use o3::depot::capacity;

use crate::{
    Errno,
    io::socket::{option, option::tuning::Error},
};

/// Selects the native owner for one affine socket operation.
pub trait Source {
    type Socket<H>: Owner<H>;
}

/// Owns the host's descriptor, route and exclusive backend access until settlement.
pub trait Owner<H>: Sized {
    fn new(host: H) -> Self;
}

/// Retains one nonempty option set until its exact native chain is selected.
pub trait Options<const N: usize>: Into<Self::Cursor> {
    type Cursor: option::Cursor;

    fn split(self) -> (capacity::Limit<N>, Self::Cursor);
}

/// Defers a typed flight reservation until native queue capacity is retained.
pub trait Reserve {
    type Key: Copy;
    type Vacant;
    type Reservation;
    type Error;

    fn try_vacancy(self) -> Result<Self::Vacant, Self::Error>;
    fn reserve(vacant: Self::Vacant) -> (Self::Key, Self::Reservation);

    fn try_reserve(self) -> Result<(Self::Key, Self::Reservation), Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::reserve(self.try_vacancy()?))
    }
}

/// The descriptor is returned with the exact native cancellation authority.
#[derive(Debug)]
pub enum Finish<F, R, P> {
    Applied(F),
    Pending(F, R, P),
}

/// Native option failures retain the rejected descriptor and exact failed stage.
#[derive(Debug)]
pub enum Failure<F, E> {
    Option(F, Errno),
    Reserve(Error<F, E>),
}

/// One descriptor's admitted slot in its exact exclusively borrowed backend.
///
/// Only an unsafe host's root callback can create this owner; consuming it is
/// the only way to separate its descriptor from the backend and native target.
pub struct Admission<'a, B, F, S, I> {
    backend: &'a mut B,
    file: F,
    slot: S,
    index: I,
}

impl<'a, B, F, S, I> Admission<'a, B, F, S, I> {
    pub fn into_parts(self) -> (&'a mut B, F, S, I) {
        (self.backend, self.file, self.slot, self.index)
    }

    pub fn into_file(self) -> F {
        self.file
    }
}
