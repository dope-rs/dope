//! Compact affine representations of reply resource ownership.

mod plan;
pub(in crate::reply) use plan::Plan;

pub(in crate::reply) mod entries;
pub(in crate::reply) mod payload;
