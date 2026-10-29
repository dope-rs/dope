mod lease;

use std::num::NonZero;

pub use lease::Lease;

use crate::core::driver::route::table::conn;

/// Fixed receive storage and per-connection retention limits.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub slots: NonZero<u32>,
    pub chunks: NonZero<u32>,
    pub bytes: NonZero<u32>,
}

/// Every connection needs one reserved receive slot.
#[derive(Clone, Copy, Debug)]
pub struct Capacity {
    pub connections: conn::Capacity,
    pub slots: NonZero<u32>,
}
