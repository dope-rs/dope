use std::{ops, task, time};

use dope_sys::{
    engine,
    io::reactor::ready::{self, Backend as _, delivery, store::Quota},
};
pub(in crate::reactor) use lanes::{Change, Completion, Resume};
use o3::mem::quota::Admission;

use crate::{Kqueue, reactor::Engine};

mod lanes;

#[derive(Debug)]
pub struct Work<Q> {
    quota: Q,
    floor: usize,
}

impl<Q: Quota> Work<Q> {
    unsafe fn poll<'a, C>(
        &self,
        backend: &'a mut Engine<C>,
        span: Option<time::Duration>,
    ) -> Result<(), <Engine<C> as ready::Backend<C>>::Failure>
    where
        C: ready::Driver<Context: ready::raw::Decode + 'a>,
    {
        let cap = backend
            .change_len()
            .min(self.quota.remaining().saturating_sub(self.floor));
        {
            let mut edits = self.quota.budget::<Change>(cap);
            let cap = self.quota.remaining().min(backend.available());
            let mut done = self.quota.budget::<Completion>(cap);
            unsafe { ready::raw::Poll::wait(backend, span, &mut edits, &mut done) }?;
        }
        unsafe { self.resume(backend, self.quota.remaining()) };
        Ok(())
    }

    unsafe fn resume<'a, C>(&self, backend: &'a mut Engine<C>, limit: usize)
    where
        C: ready::Driver<Context: ready::raw::Decode + 'a>,
    {
        let limit = limit.min(backend.available());
        let mut budget = self.quota.budget::<Resume>(limit);
        unsafe { ready::raw::Poll::resume(backend, &mut budget) };
    }
}

#[derive(Debug)]
pub struct Dispatch<'a, H, Q> {
    host: &'a mut H,
    quota: Q,
}

mod raw;
