use o3::buffer;

use crate::{
    io,
    io::reactor::{complete, ready::delivery},
    memory::{self, buffer::Recycle},
};

pub mod admit;
mod cancel;
mod install;
pub mod life;
pub mod raw;
pub mod register;
pub mod ring;
pub mod shutdown;
pub use cancel::Cancel;
pub use install::Install;

/// Exact completed receive ownership and its original projection context.
pub trait Buffer {
    type Layout: memory::raw::Layout;
    type Token: Recycle;
    type Context: memory::Context<Layout = Self::Layout> + buffer::raw::Fixed<Self::Token>;
    type Selection: Copy + Eq;
}

/// Selects the native backend owner for its host's typed contracts.
pub trait Source<C> {
    type Map: complete::Map;
    type Backend: io::Close<State = Self::Closing> + shutdown::Shutdown;
    type Closing: io::Retry;
}

/// Native completion queue and the pending buffer owner returned by that queue.
pub trait Complete {
    type Map: complete::Map;
    type Pending: Recycle;

    /// Consumes at most one native completion: `None` is empty, `Some(None)` is
    /// one internal step, and `Some(Some(entry))` transfers one host-visible entry.
    fn next(&mut self) -> Option<Option<<Self::Map as complete::Map>::Entry<Self::Pending>>>;
}

/// Selects one native turn that retains the host and its scheduling quota.
pub trait Dispatch<'a, C, H>: Source<C>
where
    C: delivery::Context<H, Backend = Self::Backend>,
    H: 'a,
{
    type Turn<Q: delivery::Quota<H>>: raw::Dispatch<'a, C, H, Q>;
}

/// Selects retained construction and fixed-file removal owners for one host.
pub trait Storage<C, P: Copy, R: Copy>: Removal {
    type Cleanup: memory::Cleanup<P>;
    type Release: memory::Cleanup<(R, Self::Slot)>;
}

/// Selects the native fixed-file target retained by a failed removal.
pub trait Removal {
    type Slot: Copy + Eq;
}
