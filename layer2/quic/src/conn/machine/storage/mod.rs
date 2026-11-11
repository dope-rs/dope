//! Demand-backed connection journal storage with stable generational identity.

pub(in crate::conn) use o3::depot::arena::slab::key::Handle as Key;
pub(in crate::conn) use plan::Plan;

pub(in crate::conn) mod capacity;
pub(in crate::conn) mod compact;
pub(in crate::conn) mod demand;
mod plan;
