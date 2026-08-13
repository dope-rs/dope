use std::{io, time};

use crate::driver::schedule;

mod backend;
mod dispatch;
mod source;

pub(crate) use backend::Backend;
pub use dispatch::Dispatch;
pub use source::Source;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "pending backend events must be drained before blocking"]
pub enum Drain {
    /// The backend's visible event queue was empty after synchronization.
    Drained,
    /// Visible events remain and require another reactor turn.
    Pending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "pending backend changes must be committed in a later reactor turn"]
pub enum Commit {
    /// Every change queued before this call has reached the backend.
    Drained,
    /// Queued changes remain and require another reactor turn.
    Pending,
}

pub trait Poll<'d>: Backend {
    fn commit(&mut self, work: schedule::Reactor<'_, 'd>) -> io::Result<Commit>;

    fn wait(
        &mut self,
        work: schedule::Reactor<'_, 'd>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()>;
}
