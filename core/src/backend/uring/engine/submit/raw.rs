use io_uring::squeue;

use crate::{backend::uring::ring, driver};

pub(in crate::backend::uring) struct Batch<'entries>(&'entries [squeue::Entry]);

impl<'entries> Batch<'entries> {
    /// The caller proves that every address captured by `entries` remains live
    /// until its terminal completion or ring quiescence.
    pub(in crate::backend::uring) unsafe fn new(entries: &'entries [squeue::Entry]) -> Self {
        Self(entries)
    }

    pub(in crate::backend::uring) fn submit(
        self,
        ring: &mut ring::Ready,
    ) -> Result<(), driver::SubmitError> {
        ring.push_multiple(self.0).map_err(|_| driver::SubmitError)
    }
}
