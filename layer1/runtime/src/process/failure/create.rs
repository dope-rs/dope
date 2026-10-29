//! Process runtime construction failures.

use o3::alloc;

use crate::process::failure::config;

/// Failure to allocate or validate a process runtime.

#[derive(Debug)]
pub enum Error {
    Config(config::Error),
    Allocation(alloc::Error),
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}
