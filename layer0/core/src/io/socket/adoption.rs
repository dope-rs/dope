use dope_sys::Errno;

use crate::io::socket::install::adopt;

/// Failure to validate the semantic role of an adopted descriptor.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Error {
    Syscall(Errno),
    Role(adopt::Error),
}
