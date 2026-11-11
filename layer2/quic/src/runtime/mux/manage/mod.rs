use std::mem;

pub mod config;
mod cursor;
mod shutdown;

use cursor::Cursor;
pub use shutdown::Shutdown;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GsoError {
    Unavailable,
}

pub(in crate::runtime) fn gso_limits() -> Result<dope::core::io::datagram::GsoLimits, GsoError> {
    dope::manifold::network::datagram::GSO_LIMITS
        .filter(|limits| limits.capacity().count() <= usize::from(u8::MAX))
        .ok_or(GsoError::Unavailable)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Status {
    Complete,
    Pending,
}

#[derive(Default)]
pub(in crate::runtime::mux) struct State {
    pub(in crate::runtime::mux) shutting_down: bool,
    pub(in crate::runtime::mux) admission_closed: bool,
    pub(in crate::runtime::mux) cursor: Cursor,
}

const _: () = assert!(mem::size_of::<State>() == 2 * mem::size_of::<usize>());
