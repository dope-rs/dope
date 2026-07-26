#![warn(unreachable_pub)]

extern crate self as dope_fiber;

pub mod abi;
pub mod extensions;
pub mod file;
pub mod io;
pub mod net;
pub mod one_shot;
pub mod owner;
pub mod raw;
pub mod slab;
pub mod sleep;
pub mod wait;

use abi::{Fiber, IntoFiber};
pub use dope_gen::{fiber, fiber_fn};
use one_shot::OneShot;
use raw::task::{Context, Waker};
use raw::wait::{WaitQueue, Waiter};
