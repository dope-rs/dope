use dope_core::Errno;
use dope_runtime::process::{failure::bus, lifecycle};

pub(crate) use lifecycle::scope;
pub use lifecycle::{Entry, Join, Start, Task};

use crate::harness::scenario::process::deadline;

/// Exact failure boundary of a managed test scenario.
#[derive(Debug)]
pub enum Error<E> {
    Deadline(deadline::Error),
    Start(Start),
    ShutdownCreate(bus::Create),
    ShutdownFire(Errno),
    Readiness(lifecycle::WaitError),
    Server {
        output: Result<(), E>,
        report: bus::Report,
    },
}
