use o3::buffer::resident::staging;

use crate::client::session::codec::staging::{Limit, Seal};

/// Selects the exact allocation and buffer shape for one parser staging mode.
pub trait Policy: Seal {
    type Spec: Copy;
    type Plan: Copy;

    /// Validates a public staging specification.
    fn plan(spec: Self::Spec) -> Result<Self::Plan, staging::Error>;

    /// Preserves the staging mode and its exact validated growth ceiling.
    fn limit(plan: Self::Plan) -> Limit;
}
