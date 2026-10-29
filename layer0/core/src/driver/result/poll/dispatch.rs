use std::task;

use poll::fault;

use crate::{driver::result::poll, io};

/// Direct source dispatch retaining at most one materialized event while all
/// remaining completions stay in the backend's compact queue.
#[must_use = "a retained event or pending source must be driven before blocking"]
pub enum Dispatch<'d> {
    Ready {
        drain: poll::Drain,
        retained: Option<io::Event<'d>>,
    },
    Fault(fault::Dispatch),
}

impl<'d> From<(task::Poll<()>, Option<io::Event<'d>>)> for Dispatch<'d> {
    fn from((state, retained): (task::Poll<()>, Option<io::Event<'d>>)) -> Self {
        Self::Ready {
            drain: state.into(),
            retained,
        }
    }
}

impl From<(task::Poll<()>, fault::Fault)> for Dispatch<'_> {
    fn from((state, failure): (task::Poll<()>, fault::Fault)) -> Self {
        Self::Fault(fault::Dispatch::new(state.into(), failure))
    }
}
