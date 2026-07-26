pub(in crate::net) mod arena;
pub(in crate::net::port) mod queue;
mod slots;

use dope::io::provided::ProvidedView;
use o3::buffer::{Bytes, Retained};

const NONE: u32 = u32::MAX;

pub(in crate::net::port) enum Buffer<'d> {
    Owned(Bytes<Retained>),
    Provided(ProvidedView<'d>),
}

impl Buffer<'_> {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(value) => value.as_slice(),
            Self::Provided(value) => value.as_slice(),
        }
    }

    pub(super) fn advance(&mut self, count: usize) {
        match self {
            Self::Owned(value) => value.advance(count),
            Self::Provided(value) => value.advance(count),
        }
    }
}
