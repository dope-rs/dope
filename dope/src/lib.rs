#![warn(unreachable_pub)]

extern crate self as dope;

pub mod hash;
pub mod manifold;
pub mod panic;
pub mod runtime;

pub use dope_core::backend::{Sqe, TimerSpec};
pub use dope_core::driver::completion::Completion;
pub use dope_core::driver::submission::Submission;
pub use dope_core::driver::{DriverContext, DriverRef};
pub use dope_core::io::{Event, EventKind, EventRef};
pub use dope_core::{driver, io};
