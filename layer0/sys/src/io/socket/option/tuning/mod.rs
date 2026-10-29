mod create;
pub mod device;
mod error;
mod missing;
mod occupied;
pub mod raw;
mod source;
mod table;

pub use create::Create;
pub use error::Error;
pub use missing::Missing;
pub use occupied::Occupied;
pub use source::Source;
pub use table::Table;

/// Observes whether every pinned option chain has released its target.
pub trait Quiesce {
    fn is_quiescent(&self) -> bool;
}
