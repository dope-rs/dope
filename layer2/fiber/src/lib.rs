//! Driver-scoped cooperative async built by rustc.
//! [`Fiber`](abi::Fiber) is the application async ABI: [`fiber!`] and [`fiber_fn`] retain Rust's async language and rustc state machines. Application awaits accept a `Fiber` bound to the same driver scope; immediately awaited non-move `async` scopes share the enclosing brand and budget. Standard future and waker machinery are a sealed compiler bridge, never an interoperability surface.
//! The bridge preserves exact lifetime and scope identity and the native future's layout without heap allocation or synchronization. A stack-local frame delegates the admitted initial descent; subsequent awaits consume application credits. For nesting depth D and budget B, traversal is bounded by O(B × D) under the fibers' cooperative-work contract. Scheduling, wakeups, cancellation, and typed failures stay in the driver context; standalone polling uses [`abi::Task`] to retain completion ownership.

use dope::{core, manifold, runtime};
use runtime::{
    executor,
    executor::{drive, session},
};

pub mod abi;
pub mod context;
pub mod net;
pub mod reply;
pub mod task;
pub mod wait;

pub use dope_gen::{fiber, fiber_fn};

pub trait BlockOn<'d> {
    fn block_on<F>(&mut self, fiber: F) -> Result<F::Output, drive::Error>
    where
        F: abi::Fiber<'d>;
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> BlockOn<'d> for session::App<'app, 'life, 'd, D, Q>
where
    D: executor::raw::App<'d>,
{
    fn block_on<F>(&mut self, fiber: F) -> Result<F::Output, drive::Error>
    where
        F: abi::Fiber<'d>,
    {
        use task::done;

        self.drive(done::Once::new(fiber))
    }
}
