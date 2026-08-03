#![warn(unreachable_pub)]

extern crate self as dope_fiber;

pub mod abi;
pub mod extensions;
pub mod file;
pub mod io;
pub mod local;
pub mod net;
pub mod notify;
pub mod one_shot;
pub mod raw;
pub mod set;
pub mod slab;
pub mod sleep;
pub mod wait;

use abi::{Fiber, IntoFiber};
pub use dope_gen::{fiber, fiber_fn};
use one_shot::OneShot;
use raw::task::Context;
use raw::wait::{WaitQueue, Waiter};
