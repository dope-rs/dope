use std::convert::Infallible;

use o3::buffer::{self, pool};

use crate::memory::{self, buffer::Recycle};

/// A native receive pool paired with its exact projection context.
pub trait Pool: Sized {
    type Layout: memory::raw::Layout;
    type Buffer: crate::Buffer + Recycle<Deferred = Infallible>;
    type Context: memory::Context<Layout = Self::Layout> + buffer::raw::Fixed<Self::Buffer>;

    fn try_new(layout: Self::Layout) -> Result<(Self, Self::Context), pool::CreateError>;
    fn take(&self) -> Option<Self::Buffer>;
}
