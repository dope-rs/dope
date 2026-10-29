use std::mem;

use dope_sys::engine;

use crate::{Engine, driver::result::poll};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Error(<Engine as engine::raw::Poll>::Error);

impl Error {
    pub(crate) const fn from_backend(error: <Engine as engine::raw::Poll>::Error) -> Self {
        Self(error)
    }

    pub fn into_cause(self) -> poll::Cause {
        poll::Cause::from(self.0)
    }
}

const _: () = {
    assert!(mem::size_of::<Error>() == mem::size_of::<<Engine as engine::raw::Poll>::Error>());
    assert!(mem::align_of::<Error>() == mem::align_of::<<Engine as engine::raw::Poll>::Error>());
};
