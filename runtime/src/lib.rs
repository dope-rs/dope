pub mod client;
pub mod coordinate;
pub mod executor;
pub mod process;
pub mod random;
mod run;
mod sealed;
pub mod shutdown;

use dope_core::io;
pub(crate) use sealed::{Installed, Owner};

/// The only completion materialized outside its backend source queue.
type Events<'d> = Option<io::Event<'d>>;

const _: () =
    assert!(std::mem::size_of::<Events<'static>>() == std::mem::size_of::<io::Event<'static>>());
