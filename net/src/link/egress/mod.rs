pub mod arena;
pub mod config;
mod entry;
mod flight;
pub mod metadata;
pub mod queue;
pub(crate) mod stable;
pub mod stage;
pub mod storage;
mod wire;

pub use stable::{LeaseBuffer, StableBytes, StaticBytes};

type WirePool = o3::buffer::Pool<o3::buffer::FixedPoolCapacity<{ o3::buffer::BLOCK_CAPACITY }>>;
type WireLease<'pool> =
    o3::buffer::Lease<'pool, o3::buffer::FixedPoolCapacity<{ o3::buffer::BLOCK_CAPACITY }>>;

const EGRESS_CAP_BYTES: u32 = 1 << 20;
const EGRESS_QUANTUM: usize = 256;
const EGRESS_CAP_ENTRIES: u32 = EGRESS_CAP_BYTES / EGRESS_QUANTUM as u32;
