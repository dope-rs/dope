use std::time;

use dope_core::Errno;
use dope_runtime::process::{failure::bus, lifecycle::WaitError};

use crate::harness::scenario::run;

#[derive(Debug)]
pub enum Error {
    Overflow {
        duration: time::Duration,
    },
    Create(bus::Create),
    Start(run::Start),
    Cancel(Errno),
    Worker {
        cancel: Option<Errno>,
        output: Result<(), WaitError>,
        report: bus::Report,
    },
}
