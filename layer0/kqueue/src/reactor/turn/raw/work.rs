use dope_sys::io::reactor::ready::Backend as _;

use crate::reactor::turn::{Engine, Quota, Work, ready, task, time};

impl<C, Q> ready::raw::Work<C, Q> for Work<Q>
where
    C: ready::Driver<Context: ready::raw::Decode>,
    Q: Quota,
{
    type Backend = Engine<C>;

    unsafe fn new<'a>(_: &'a mut Self::Backend, quota: Q) -> Self
    where
        C::Context: 'a,
    {
        Self { quota }
    }

    unsafe fn commit<'a>(
        self,
        backend: &'a mut Self::Backend,
        dirty: bool,
    ) -> Result<task::Poll<()>, <Self::Backend as ready::Backend<C>>::Failure>
    where
        C::Context: 'a,
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

    unsafe fn wait<'a>(
        self,
        backend: &'a mut Self::Backend,
        span: Option<time::Duration>,
        busy: bool,
    ) -> Result<(), <Self::Backend as ready::Backend<C>>::Failure>
    where
        C::Context: 'a,
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
