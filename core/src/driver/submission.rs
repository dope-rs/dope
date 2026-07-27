use crate::backend::ops::raw::submission::SubmissionBackend;
use crate::backend::{Backend, Sqe};

use super::{DriverContext, PushError};

pub mod raw {
    use crate::backend::ops::raw::submission::SubmissionBackend;
    use crate::backend::{Backend, RawSqe};

    use super::{DriverContext, PushError};

    pub trait Submission {
        /// # Safety
        /// Captured addresses obey their validity and aliasing contract through completion.
        ///
        /// ```compile_fail
        /// use dope_core::{backend::RawSqe, driver::{DriverContext, submission::Submission as _}};
        /// fn reject(driver: &mut DriverContext<'_, '_>, sqe: RawSqe) { driver.push(sqe); }
        /// ```
        unsafe fn push_raw(&mut self, sqe: RawSqe) -> Result<(), PushError>;
    }

    impl Submission for DriverContext<'_, '_> {
        unsafe fn push_raw(&mut self, sqe: RawSqe) -> Result<(), PushError> {
            <Backend as SubmissionBackend>::push(self.backend(), sqe.into_sqe())
        }
    }
}

pub trait Submission {
    fn push(&mut self, sqe: Sqe) -> Result<(), PushError>;
    fn flush_submissions(&mut self) -> bool;
}

impl Submission for DriverContext<'_, '_> {
    fn push(&mut self, sqe: Sqe) -> Result<(), PushError> {
        <Backend as SubmissionBackend>::push(self.backend(), sqe)
    }

    fn flush_submissions(&mut self) -> bool {
        <Backend as SubmissionBackend>::flush_submissions(self.backend())
    }
}
