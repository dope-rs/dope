#![warn(unreachable_pub)]

extern crate self as dope_fiber;

pub mod abi;
pub mod extensions;
pub mod file;
pub mod io;
pub mod net;
pub mod one_shot;
pub mod owner;
pub mod slab;
pub mod sleep;
pub mod task;
pub mod wait;

use abi::{Fiber, IntoFiber};
use dope::manifold::env::Bundle;
use dope::runtime::profile::Balanced;
pub use dope_gen::{fiber, fiber_fn};
use io::Io;
use one_shot::OneShot;
use task::{Context, TaskContext, Waker};
use wait::{WaitQueue, Waiter};

pub(crate) type ConnEnv<T, W> = Bundle<T, W, Balanced>;
