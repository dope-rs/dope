use crate::io::socket::route::{Attach, Plan, install::Failure};

/// Selects routing authority without exposing the native program owner.
pub trait Source {
    type Install: Attach;

    fn try_prepare(plan: Plan) -> Result<Self::Install, Failure>;
}
