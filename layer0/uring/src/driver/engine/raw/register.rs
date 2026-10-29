use std::os::fd;

use dope_sys::{
    engine::{self, register, ring},
    io::reactor::submit::{Control, Poll as _, Queue as _},
    ring::ready::Ready as _,
    table::token::Codec as _,
};

use crate::{Engine, Uring};

impl<C: ring::Context> engine::raw::Register for Engine<C> {
    unsafe fn register(&mut self, source: fd::BorrowedFd<'_>) -> Result<(), register::Error> {
        let entry = <Uring as Control>::Poll::new(source, C::Token::shutdown());
        let vacancy = self
            .ring
            .submission()
            .try_one()
            .map_err(register::Error::Full)?;
        // SAFETY: the caller retains this exact descriptor through quiescence.
        unsafe { Self::arm(vacancy, &entry) };
        Ok(())
    }
}
