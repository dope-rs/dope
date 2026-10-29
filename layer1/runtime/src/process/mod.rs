use std::num::NonZero;

use dope_core::dope_sys::thread;

mod context;
pub mod failure;
pub mod gate;
mod layout;
mod runtime;
mod shutdown;
mod terminal;
pub mod watch;

pub use context::Context;
pub use layout::Layout;
pub use runtime::Runtime;
pub use shutdown::Shutdown;
pub use terminal::Terminal;

/// Stack extent shared by process workers and their matching test workers.
pub const STACK: NonZero<usize> = {
    let bytes = NonZero::<usize>::MIN.saturating_add(8 * 1024 * 1024 - 1);
    assert!(bytes.get() >= thread::STACK_MIN);
    bytes
};
