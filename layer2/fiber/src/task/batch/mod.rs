use crate::core::driver::schedule::ready::task::{self, pool::RootId};

mod error;
mod index;
mod join;
mod pool;
mod state;

pub use error::Error;
pub use index::Index;
pub use join::Join;
pub use pool::Pool;
pub(in crate::task) use state::Completed;

enum Tag {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootError {
    Foreign,
    Invalid(task::Error),
    Changed { active: RootId, observed: RootId },
}
