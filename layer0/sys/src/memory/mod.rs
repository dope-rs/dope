pub mod raw;

pub mod buffer;
mod cleanup;
mod error;
mod failure;
pub use cleanup::Cleanup;
pub use error::Error;
pub use failure::Failure;

/// The immutable layout retained by a native receive projection.
pub trait Context {
    type Layout: raw::Layout;

    fn layout(&self) -> Self::Layout;
}
