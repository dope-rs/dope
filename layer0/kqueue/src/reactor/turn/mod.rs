use std::{ops, task, time};

use dope_sys::{
    Budget as _, engine,
    io::reactor::ready::{self, Backend as _, delivery, store::Quota},
};
pub(in crate::reactor) use lanes::{Change, Completion, Resume};
use o3::mem::quota::Admission;

use crate::{
    Kqueue,
    reactor::{CAPACITY, Engine},
};

mod lanes;

#[derive(Debug)]
pub struct Work<Q> {
    quota: Q,
}

impl<Q: Quota> Work<Q> {
    unsafe fn poll<'a, C>(
        &self,
        backend: &'a mut Engine<C>,
        span: Option<time::Duration>,
        changes: usize,
        done: usize,
    ) -> Result<bool, <Engine<C> as ready::Backend<C>>::Failure>
    where
        C: ready::Driver<Context: ready::raw::Decode + 'a>,
    {
        let mut edits = self
            .quota
            .budget::<Change>(changes.min(backend.change_len()));
        let cap = done
            .min(self.quota.remaining())
            .min(backend.available())
            .min(CAPACITY);
        let mut done = self.quota.budget::<Completion>(cap);
        unsafe { ready::raw::Poll::wait(backend, span, &mut edits, &mut done) }?;
        // A zero cap leaves readiness unobserved; a full batch may retain more.
        Ok(cap == 0 || done.remaining() == 0)
    }
}

#[derive(Debug)]
pub struct Dispatch<'a, H, Q> {
    host: &'a mut H,
    quota: Q,
}

mod raw;
