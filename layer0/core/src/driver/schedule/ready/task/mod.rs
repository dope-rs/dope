mod domain;
mod error;
pub mod owner;
mod parent;
/// Fixed-capacity task roots and borrowed admission scopes.
pub mod pool;
/// Unsafe task-binding primitives.
pub mod raw;
/// Stable ready-topology ownership proofs.
pub mod topology;

pub use domain::Domain;
pub use error::Error;
pub use parent::Parent;
/// Maximum task-to-parent edges followed by one wake.
pub const MAX_WAKE_HOPS: usize = 16;

/// Whether one live task was newly queued or was already ready.
#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use = "the activation outcome must be handled"]
pub enum Activation {
    Queued,
    Coalesced,
}

/// Whether a reclaimed task slot remains reusable after generation advance.
#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use = "retired task capacity must not be reused"]
pub enum Reclaim {
    Reusable,
    Retired,
}
