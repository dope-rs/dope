use o3::base::num::bounded;

use crate::{
    io::{Close, reactor::complete},
    memory,
    ring::{Access, map},
};
mod occupied;
mod registry;
mod remove;

pub use occupied::Occupied;
pub use registry::Registry;
pub use remove::Remove;

/// Removing the current thread's registered ring descriptor from the kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Unregister;

/// Registered CQ publication remains pending until published or refreshed.
pub trait Registered: Access + Close {
    type Entry: complete::Entry<Token = Self::Token>;

    fn acquire(&mut self);
    fn pop(&mut self) -> Option<Self::Entry>;
    fn publish(&mut self);
    fn refresh(&mut self) -> bounded::U32<0, { u32::MAX }>;
}

/// Registration failure owns the ring until its mappings have closed.
pub trait Registration: Sized {
    type Error: Copy;
    type Unmap;
    type Cleanup: memory::Cleanup<Self::Error, Error = map::Failure<Self::Unmap>>;

    fn try_close(self) -> Result<Self::Error, Self::Cleanup>;
}
