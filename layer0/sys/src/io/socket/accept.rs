use std::num;

use crate::{Cloexec, Errno, io::socket::Setup, table::registry};

/// Native accept failures retain the host's readiness-only terminal cases.
pub trait Source {
    type Failure<F: From<Errno>>: From<Errno> + Into<F>;

    /// Maximum simultaneous native accept operations on one listener.
    const MAX_IN_FLIGHT: num::NonZero<usize>;

    /// Whether one native accept failure permits another attempt on the listener.
    /// Resource exhaustion never permits an unconditional retry; retain its exact error.
    fn is_retryable(error: Errno) -> bool;

    /// Whether actual descriptor retirement permits one later attempt after this error.
    /// This does not establish a pending retirement or identify the error's origin.
    fn can_defer(error: Errno) -> bool;
}

#[derive(Debug)]
pub enum Error {
    Syscall(Errno),
    Registration(registry::Failure),
    Cloexec(Cloexec),
    Setup(Setup),
    Capacity { close: Result<(), Errno> },
}
