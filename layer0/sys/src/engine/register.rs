use crate::{Errno, io::reactor::Full};

/// Exact failure while arming the engine's retained shutdown source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Full(Full),
    Register(Errno),
}
