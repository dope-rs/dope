use std::{convert::Infallible, num::NonZero};
mod failure;

pub use failure::Failure;

use crate::{
    io::{self, Retry},
    kernel, memory,
    ring::{layout::Region, register},
};

/// Mapping rejection retaining its exact region, extent, and operation error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Map<E> {
    Syscall {
        region: Region,
        len: NonZero<usize>,
        code: E,
    },
    Null {
        region: Region,
        len: NonZero<usize>,
    },
}

/// Current mapping failure without erasing its native region or buffer extent.
#[derive(Debug, Eq, PartialEq)]
pub enum Storage<E> {
    Ring(Failure<E>),
    Buffer(memory::Error),
    Unregister(kernel::Errno<register::Unregister>),
}

/// A retained mapping cleanup state reports its current exact failure.
pub trait Unmap: Retry<Failure: Into<Storage<Self::Error>>> {
    type Error;
    fn failure(&self) -> Storage<Self::Error>;
}

impl Unmap for Infallible {
    type Error = Infallible;

    fn failure(&self) -> Storage<Self::Error> {
        match *self {}
    }
}

impl<E> From<Infallible> for Storage<E> {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

impl<E, R: Into<Storage<E>>, S: Into<memory::Error>> From<io::Cleanup<R, S>> for Storage<E> {
    fn from(value: io::Cleanup<R, S>) -> Self {
        match value {
            io::Cleanup::Owner(failure) => failure.into(),
            io::Cleanup::Storage(failure) => Self::Buffer(failure.into()),
        }
    }
}

impl<E> From<Failure<E>> for Storage<E> {
    fn from(cause: Failure<E>) -> Self {
        Self::Ring(cause)
    }
}

impl<E> From<kernel::Errno<register::Unregister>> for Storage<E> {
    fn from(cause: kernel::Errno<register::Unregister>) -> Self {
        Self::Unregister(cause)
    }
}
