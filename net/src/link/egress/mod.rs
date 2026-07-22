pub mod arena;
pub mod config;
mod credits;
pub mod metadata;
pub mod queue;
pub(crate) mod raw;
pub mod stage;
mod wire;

pub(crate) const EGRESS_CAP_BYTES: u32 = 1 << 20;
pub(crate) const EGRESS_QUANTUM: usize = 256;
pub(crate) const EGRESS_CAP_ENTRIES: u32 = EGRESS_CAP_BYTES / EGRESS_QUANTUM as u32;
const NONE: u32 = u32::MAX;
