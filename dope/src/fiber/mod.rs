use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;

use crate::backend;

mod connector;
pub mod file;
mod holding;
mod io;
mod listener;
pub(crate) mod pending;
mod slab;
mod sleep;
mod state;
mod wake;

pub use connector::Connector;
pub use holding::Holding;
pub use io::Io;
pub use listener::Listener;
pub use o3::task::{Batch, Lazy};
pub use slab::{Slab, TaskId};
pub use sleep::Sleep;
pub use wake::WakerSet;

pub(super) type ConnEnv<T, W> = crate::manifold::env::Bundle<T, W, backend::profile::Production>;

#[pin_project]
pub struct Fiber<'d, F: Future> {
    #[pin]
    inner: F,
    _brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F: Future> Fiber<'d, F> {
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            _brand: PhantomData,
        }
    }
}

impl<'d, F: Future> Future for Fiber<'d, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.project().inner.poll(cx)
    }
}
