use std::{task, time};

use dope_sys::{
    engine::{self, ring},
    io::reactor::{ready::store::Quota, submit::Queue as _},
    ring::{Source, ready::Ready as _},
};

use crate::{
    Engine, Uring,
    driver::tuning::{self, Progress as _},
};

impl<C: ring::Context> engine::raw::Poll for Engine<C>
where
    C::Tuning: tuning::Progress<C::Token>,
{
    type Context = C;
    type Error = <Uring as Source>::Poll;

    unsafe fn commit<'a, Q: Quota>(
        &'a mut self,
        quota: Q,
        deferred: impl FnOnce() -> bool,
    ) -> Result<task::Poll<()>, Self::Error>
    where
        C: 'a,
    {
        quota.refund();
        let busy = deferred() || self.tuning.has_pending();
        self.ring.commit()?;
        Ok(if busy || !self.ring.submission().is_empty() {
            task::Poll::Pending
        } else {
            task::Poll::Ready(())
        })
    }

    unsafe fn wait<'a, Q: Quota>(
        &'a mut self,
        quota: Q,
        timeout: Option<time::Duration>,
        busy: impl FnOnce() -> bool,
    ) -> Result<(), Self::Error>
    where
        C: 'a,
    {
        quota.refund();
        if busy() || self.tuning.has_pending() || timeout.is_some_and(|span| span.is_zero()) {
            self.ring.commit()
        } else {
            self.ring.try_wait(timeout)
        }
    }
}
