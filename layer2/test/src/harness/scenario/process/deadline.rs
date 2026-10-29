use std::time;

use dope_core::platform::wake;

use crate::harness::scenario::run;

#[derive(Debug)]
pub enum Error {
    Overflow {
        duration: time::Duration,
    },
    Create(wake::CreateError),
    Start(run::Start),
    Join {
        cancel: Option<wake::NotifyError>,
        cause: run::Join,
    },
    Cancel(wake::NotifyError),
    Wait(wake::WaitError),
    Both {
        cancel: wake::NotifyError,
        wait: wake::WaitError,
    },
}
