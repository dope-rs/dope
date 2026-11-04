use dope_core::platform::wake;

use crate::harness::peer::{Error, Spawn};

#[derive(Debug)]
pub enum Finish {
    Spawn(Spawn),
    Release(wake::WaitError),
    Worker(Error),
    Timeout,
}
