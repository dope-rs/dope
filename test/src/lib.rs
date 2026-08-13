use std::time;

pub mod checks;
pub mod dispatch;
pub mod fibers;
pub mod file;
mod harness;
pub mod peer;
pub mod scenario;

pub use harness::Harness;

pub const GUARD: time::Duration = time::Duration::from_secs(5);
