use crate::backend::ops::raw::submission::SubmissionBackend;
use crate::backend::{Backend, RetainedSqe, Sqe};

use super::{DriverContext, PushError};

pub trait Submission {
    fn push(&mut self, sqe: Sqe) -> Result<(), PushError>;
    fn push_retained(&mut self, sqe: RetainedSqe) -> Result<(), PushError>;
    fn flush_submissions(&mut self) -> bool;
}

impl Submission for DriverContext<'_, '_> {
    fn push(&mut self, sqe: Sqe) -> Result<(), PushError> {
        <Backend as SubmissionBackend>::push(self.backend(), sqe)
    }

    fn push_retained(&mut self, sqe: RetainedSqe) -> Result<(), PushError> {
        <Backend as SubmissionBackend>::push(self.backend(), Sqe::from_retained(sqe))
    }

    fn flush_submissions(&mut self) -> bool {
        <Backend as SubmissionBackend>::flush_submissions(self.backend())
    }
}
