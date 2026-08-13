use crate::{driver::ops::poll, io};

/// Direct source dispatch retaining at most one materialized event while all
/// remaining completions stay in the backend's compact queue.
#[doc(hidden)]
#[must_use = "a retained event or pending source must be driven before blocking"]
pub struct Dispatch<'d> {
    drain: poll::Drain,
    retained: Option<io::Event<'d>>,
}

impl<'d> Dispatch<'d> {
    pub(crate) const fn new(drain: poll::Drain, retained: Option<io::Event<'d>>) -> Self {
        Self { drain, retained }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (poll::Drain, Option<io::Event<'d>>) {
        (self.drain, self.retained)
    }
}
