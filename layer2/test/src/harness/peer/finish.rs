use dope_core::Errno;
use dope_runtime::process::{failure::bus, lifecycle::WaitError};

use crate::harness::peer::Error;

#[derive(Debug)]
pub enum Finish {
    Worker(Error),
    Lifecycle {
        wait: Option<WaitError>,
        stop: Option<Errno>,
        worker: Option<Error>,
        report: bus::Report,
    },
}
