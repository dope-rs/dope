#![warn(unreachable_pub)]

extern crate self as dope_fiber;

mod abi;
mod extensions;
pub mod file;
mod io;
mod net;
mod one_shot;
mod race;
mod slab;
mod sleep;
mod task;
mod wait;

pub use abi::{
    __private, Batch, BatchOutput, Fiber, IntoFiber, Lazy, Pending, PollFn, Ready, WaitFn, pending,
    poll_fn, ready, wait_fn,
};
use dope::manifold::env::Bundle;
use dope::runtime::profile::Balanced;
pub use dope_gen::{fiber, fiber_fn};
pub use extensions::{AppSessionExt, SessionExt, SlotExt};
pub use io::{Io, RecvBuffer};
pub use net::connector::{
    Connect, Connector, ConnectorHandle, ConnectorPort, ConnectorPortFactory,
};
pub use net::listener::{Listener, ListenerHandle, ListenerPort, ListenerPortFactory};
pub use one_shot::OneShot;
pub use race::{Either, Race};
pub use slab::{ErasedTaskId, FixedSlab, FixedSlabVacantEntry, Slab, SlabVacantEntry, TaskId};
pub use sleep::{Sleep, TimerExt};
pub use task::{Context, TaskContext, TaskQueue, Waker};
pub use wait::{WaitQueue, Waiter};

pub const fn race<L, R>(left: L, right: R) -> Race<L, R> {
    Race::new(left, right)
}

pub(crate) type ConnEnv<T, W> = Bundle<T, W, Balanced>;
