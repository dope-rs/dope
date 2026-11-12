use std::{convert::Infallible, fmt::Debug};

use dope_runtime::topology::error;

pub mod client;
pub mod datagram;
pub mod listener;

pub fn native<R: Debug>(outcome: Result<R, error::Kind<'_, Infallible, &R>>) -> R {
    outcome.expect("native lifecycle")
}

pub struct Pattern(pub(crate) usize);
impl Pattern {
    pub fn bytes(self) -> Vec<u8> {
        (0..self.0).map(|index| (index % 251) as u8).collect()
    }
}
mod host;
pub use host::Host;
pub mod profile;
pub mod root;
mod topology;
pub use topology::Topology;
