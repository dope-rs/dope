use o3::buffer::{bytes::bounded::Overflow, storage};
pub(super) mod credit;
pub mod lease;
mod value;

pub use value::Value;

use crate::link::egress;

/// Exact failure of the first payload geometry check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty(storage::Zero),
    Transfer(Overflow),
    Resident(egress::ResidentOut),
}
