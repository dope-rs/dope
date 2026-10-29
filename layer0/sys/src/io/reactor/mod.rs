pub mod complete;
pub mod read;
pub mod ready;
pub mod retry;
pub mod submit;
use crate::Errno;

/// No queue admission is available for this submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Full;

/// Failure while creating a kernel event queue.
#[derive(Debug)]
pub enum Create {
    Open(Errno),
    Cloexec(Errno),
}
