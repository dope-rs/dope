use std::io;

use dope_core::platform::wake;

use crate::harness::{peer::Spawn, scenario::run};

#[derive(Debug)]
pub enum Finish {
    Spawn(Spawn),
    Join(run::Join),
    Release(wake::WaitError),
    Worker(io::Error),
    Timeout,
}
