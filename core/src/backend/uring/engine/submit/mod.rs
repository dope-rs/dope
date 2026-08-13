pub(in crate::backend::uring) mod raw;

use std::mem;

use io_uring::squeue;

use crate::{
    backend::{
        self, bound,
        uring::{engine::lifecycle, ring, submission},
    },
    driver::{self, flight},
    platform::reactor,
};

#[repr(transparent)]
pub(crate) struct Queue<'a> {
    backend: &'a mut backend::Uring,
}

#[repr(transparent)]
pub(in crate::backend::uring) struct Writer<'a>(&'a mut ring::Ready);

const _: () = {
    assert!(mem::size_of::<Queue<'static>>() == mem::size_of::<&'static mut backend::Uring>());
    assert!(mem::size_of::<Writer<'static>>() == mem::size_of::<&'static mut ring::Ready>());
};

impl<'a> Queue<'a> {
    pub(crate) fn new(backend: &'a mut backend::Uring) -> Self {
        Self { backend }
    }
}

impl reactor::Queue for Queue<'_> {
    fn submit<'owner, 'd: 'owner>(
        &mut self,
        submission: bound::Bound<'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let (submission, reservation) = submission.into_parts();
        let entry = submission.bind(reservation.key());
        Writer::new(&mut self.backend.ring).submit_bound(&entry)?;
        Ok(reservation.commit())
    }

    fn cancel(&mut self, flight: &mut flight::Flight<'_>) -> Result<(), driver::SubmitError> {
        Writer::new(&mut self.backend.ring).submit(&submission::Submission::cancel(flight.key()))
    }
}

impl<'a> Writer<'a> {
    pub(in crate::backend::uring) fn new(ring: &'a mut ring::Ready) -> Self {
        Self(ring)
    }

    pub(in crate::backend::uring) fn submit_once(
        &mut self,
        submission: &submission::Submission,
    ) -> Result<(), driver::SubmitError> {
        self.0
            .push_once(submission.entry())
            .map_err(|_| driver::SubmitError)
    }

    pub(in crate::backend::uring) fn submit(
        &mut self,
        submission: &submission::Submission,
    ) -> Result<(), driver::SubmitError> {
        self.submit_entry(submission.entry())
    }

    fn submit_bound(&mut self, submission: &submission::Bound) -> Result<(), driver::SubmitError> {
        self.submit_entry(submission.entry())
    }

    fn submit_entry(&mut self, entry: &squeue::Entry) -> Result<(), driver::SubmitError> {
        self.0.push(entry).map_err(|_| driver::SubmitError)
    }

    pub(in crate::backend::uring) fn try_close(
        &mut self,
        work: lifecycle::CloseWork,
    ) -> Result<(), lifecycle::CloseWork> {
        use libc::SHUT_RDWR;
        let slot = work.slot();
        let shut = submission::Submission::shutdown_linked_at(slot, SHUT_RDWR);
        let close = submission::Submission::close_at(slot);
        let entries = [shut.entry().clone(), close.entry().clone()];
        match self.0.push_multiple_once(&entries) {
            Ok(()) => Ok(()),
            Err(_) => Err(work),
        }
    }
}
