use dope_core::{
    driver::{self, flight, retained, route},
    io::fd::handles,
};

use crate::wire::send;

/// # Safety
/// Submitted pointers must stay fixed through completion or driver quiescence.
pub(crate) unsafe trait Send {
    fn submit_retained<'owner, 'd: 'owner, Tag: route::Tag>(
        &self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        flights: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;
}

// SAFETY: Plain comes from static bytes, installed connection storage, or a
// raw retention boundary which keeps the bytes fixed through completion.
unsafe impl Send for send::Plain<'_> {
    fn submit_retained<'owner, 'd: 'owner, Tag: route::Tag>(
        &self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        flights: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let submission = retained::raw::Submission::send(fd, self.as_slice(), target)
            .map_err(|_| driver::SubmitError)?;
        // SAFETY: this trait's contract retains the bytes and Engine retains
        // the fixed descriptor until terminal completion.
        unsafe { retained::raw::Owner::submit(driver, flights, submission) }
    }
}

// SAFETY: Vectored belongs to an installed send state whose descriptor lease
// and message storage are not recycled before terminal completion.
unsafe impl Send for send::Vectored<'_> {
    fn submit_retained<'owner, 'd: 'owner, Tag: route::Tag>(
        &self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        flights: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let submission = retained::raw::Submission::send_msg(fd, self.message(), target);
        // SAFETY: this trait's contract retains the message and Engine retains
        // the fixed descriptor until terminal completion.
        unsafe { retained::raw::Owner::submit(driver, flights, submission) }
    }
}
