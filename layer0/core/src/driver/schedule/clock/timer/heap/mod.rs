//! Fixed-capacity minimum heap with independently leased resident storage.

pub use min::{Min, vacancy::Vacancy};
pub use minimum::{Minimum, fired::Fired};
pub use tokens::{lease::Lease, state::State};

use self::core::Core;

pub mod raw;

mod capacity;
mod core;
mod min;
mod minimum;
mod order;
mod tokens;

const FREE: u32 = u32::MAX;
const IDLE: u32 = u32::MAX - 1;
const FIRED: u32 = u32::MAX - 2;
