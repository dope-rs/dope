use std::time;

pub mod process;
pub mod run;

pub const TIMEOUT: time::Duration = time::Duration::from_secs(2);

mod native;
