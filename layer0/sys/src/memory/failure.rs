use std::convert::Infallible;

use o3::mem::Rejected;

use crate::memory::Error;

/// Inspects a retained mapping failure without releasing its storage owner.
pub trait Failure {
    fn failure(&self) -> Error;
}

impl Failure for Infallible {
    fn failure(&self) -> Error {
        match *self {}
    }
}

impl<T> Failure for Rejected<T, Error> {
    fn failure(&self) -> Error {
        *self.error()
    }
}
