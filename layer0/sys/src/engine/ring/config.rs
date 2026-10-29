use std::num;

use o3::{base::num::bounded, depot};

use crate::{engine::ring::Context, table::permit};

/// Construction inputs retain nonzero queue counts and the fixed-file ABI bound.
pub struct Build<L> {
    pub submit: num::NonZero<u32>,
    pub complete: num::NonZero<u32>,
    pub defer: bool,
    pub files: bounded::U32<1, { 1 << 24 }>,
    pub range: Option<bounded::U32<1, { 1 << 24 }>>,
    pub receive: L,
}

/// Projects validated host settings before native construction begins.
pub trait Config<G>: Context {
    type Key: permit::raw::Key;
    type Index: depot::raw::Compact;

    fn config(config: &G) -> Build<Self::Layout>;
}
