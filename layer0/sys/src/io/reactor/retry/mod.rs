use std::os::fd;

use crate::table::change;

mod attempt;
mod complete;
mod context;
pub mod raw;
mod source;
mod state;
mod vacancy;

pub use attempt::Attempt;
pub use complete::Complete;
pub use context::Context;
pub use source::Source;
pub use state::State;
pub use vacancy::Vacancy;

/// Borrows both retry indexes until their exact operation is removed.
pub trait Occupied: Sized {
    type Entry: Entry;

    fn record(&self) -> &<Self::Entry as Entry>::Record;
    fn remove(self) -> Self::Entry;
}

/// A pending retry retaining its exact target and operation record.
pub trait Entry: Sized {
    type Target: Copy + Eq + Into<usize>;
    type Record;
    type File: change::Id;

    fn target(&self) -> Self::Target;
    fn fd(&self) -> fd::RawFd;
    fn descriptor(record: &Self::Record) -> fd::RawFd;
    fn file(record: &Self::Record) -> Self::File;
    fn into_parts(self) -> (Self::Target, Self::Record);
    fn from_parts(target: Self::Target, record: Self::Record) -> Self;
}
