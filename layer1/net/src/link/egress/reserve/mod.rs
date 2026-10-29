mod bounds;
pub mod demand;
pub mod error;
pub mod exact;
pub(in crate::link::egress) mod guard;
pub mod limit;
mod plan;
mod vacant;

pub use bounds::Bounds;
pub(in crate::link::egress) use bounds::Entries;
pub use plan::Plan;
pub use vacant::Vacant;

/// Queue visibility produced by a validated final-link reservation.
#[derive(Clone, Copy)]
pub enum Publish {
    Published,
    Empty,
}
