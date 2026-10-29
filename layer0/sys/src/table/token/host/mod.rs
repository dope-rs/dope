use crate::table::token;

/// Exact host identity projections at the kernel token boundary.
pub trait Context: token::Context {
    type Key: Copy + Eq + Into<u64>;
    type Close<'d>: AsRef<u64>;
    type Retire<'d>: Retire;
    type Returned: From<u64>;
    type Probe: Copy + Into<u64>;

    fn read_key(raw: u64) -> Option<Self::Key>;
}

/// The encoded identity of one affine descriptor retirement.
pub trait Retire {
    fn echo(&self) -> u64;
}
/// Consumes one bound retirement completion without copying its domain authority.
pub trait Echo {
    fn complete(self);
    fn retry(self);
}
mod bound;
mod cancel;
mod closed;
mod drain;
mod flight;
pub use bound::Bound;
pub use cancel::Cancel;
pub use closed::Closed;
pub use drain::Drain;
pub use flight::Flight;
