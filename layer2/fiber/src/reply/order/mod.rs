//! Protocol ordering exposed by reply arenas.

mod depth;

pub use depth::Depth;

/// The kind of item at the front of one protocol lane.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Front {
    Empty,
    Open,
    Denied,
    Detached,
}
