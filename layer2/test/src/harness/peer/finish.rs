use dope_core::platform::wake;

use crate::harness::{
    peer::{Error, Spawn},
    scenario::run,
};

#[derive(Debug)]
pub enum Finish {
    Spawn(Spawn),
    Join(run::Join),
    Release(wake::WaitError),
    Worker(Error),
    Timeout,
}
