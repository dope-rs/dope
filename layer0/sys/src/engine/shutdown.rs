use std::convert::Infallible;

use crate::{engine::Buffer, io::Retry, memory, ring::map};

/// Ends the native reactor and its receive projection as one lifetime.
pub trait Shutdown: Buffer + Sized {
    type State: Cleanup;

    fn try_shutdown(self, context: Self::Context) -> Result<(), Self::State>;
}

/// Retains every resource needed to retry the exact failed shutdown phase.
pub trait Cleanup: Retry<Failure: Into<Failure<Self::Error>>> {
    type Error;

    fn failure(&self) -> Failure<Self::Error>;
}

/// Exact failure from the reactor or its retained receive storage.
#[derive(Debug, Eq, PartialEq)]
pub enum Failure<E> {
    Backend(map::Storage<E>),
    Context(memory::Error),
}

impl Cleanup for Infallible {
    type Error = Infallible;

    fn failure(&self) -> Failure<Self::Error> {
        match *self {}
    }
}

impl<E> From<Infallible> for Failure<E> {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}
