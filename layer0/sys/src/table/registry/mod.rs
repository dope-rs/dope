mod entry;
mod failure;
mod occupied;
mod state;
mod vacancy;

pub use entry::Entry;
pub use failure::Failure;
pub use occupied::Occupied;
pub use state::State;
pub use vacancy::Vacancy;

/// Selects a read by its exact identity or its native completion echo.
pub enum Query<E: Entry> {
    Exact(E::Id),
    Echo(E::Echo),
}
