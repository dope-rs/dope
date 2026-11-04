use dope_sys::io::reactor::ready::Backend as _;

use crate::reactor::turn::{Engine, Quota, Work, ready, task, time};

impl<Q: Quota> Work<Q> {
    pub(in crate::reactor) fn new(quota: Q) -> Self {
        Self { quota }
    }

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    pub(in crate::reactor) unsafe fn commit<'a, C>(
        self,
        backend: &'a mut Engine<C>,
        dirty: bool,
    ) -> Result<task::Poll<()>, <Engine<C> as ready::Backend<C>>::Failure>
    where
        C: ready::Driver<Context: ready::raw::Decode + 'a>,
    {
        if dirty && self.quota.remaining() != 0 {
            unsafe {
                self.poll(
                    backend,
                    Some(time::Duration::ZERO),
                    self.quota.remaining(),
                    0,
                )
            }?;
        }
        backend.check()?;
        Ok(if backend.has_changes() {
            task::Poll::Pending
        } else {
            task::Poll::Ready(())
        })
    }

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    pub(in crate::reactor) unsafe fn wait<'a, C>(
        self,
        backend: &'a mut Engine<C>,
        span: Option<time::Duration>,
        busy: bool,
    ) -> Result<(), <Engine<C> as ready::Backend<C>>::Failure>
    where
        C: ready::Driver<Context: ready::raw::Decode + 'a>,
    {
        let remaining = self.quota.remaining();
        if remaining == 0 {
            return backend.check();
        }
        let cursor = backend.cursor();
        let poll = backend.has_pending() || busy || backend.can_resume();
        let span = if poll {
            Some(time::Duration::ZERO)
        } else {
            span
        };
        if cursor == 2 {
            let span = if backend.has_changes() {
                Some(time::Duration::ZERO)
            } else {
                span
            };
            unsafe { self.poll(backend, span, 0, remaining) }?;
            unsafe {
                self.poll(
                    backend,
                    Some(time::Duration::ZERO),
                    self.quota.remaining(),
                    0,
                )
            }?;
        } else {
            unsafe { self.poll(backend, span, remaining, remaining) }?;
        }
        Ok(())
    }
}
