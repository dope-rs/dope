use dope_sys::io::reactor::ready::{Backend as _, Source as _};

use crate::reactor::turn::{Engine, Kqueue, Quota, Work, ready, task, time};

impl<C, Q> ready::raw::Work<C, Q> for Work<Q>
where
    C: ready::Driver<Context: ready::raw::Decode>,
    Q: Quota,
{
    type Backend = Engine<C>;

    unsafe fn new<'a>(backend: &'a mut Self::Backend, quota: Q) -> Self
    where
        C::Context: 'a,
    {
        let cursor = backend.cursor();
        let share = quota.share::<{ Kqueue::LANES.get() as usize }, 0>(cursor);
        let floor = quota.share::<{ Kqueue::LANES.get() as usize }, 2>(cursor);
        let work = Self { quota, floor };
        unsafe { work.resume(backend, share) };
        work
    }

    unsafe fn commit<'a>(
        self,
        backend: &'a mut Self::Backend,
        dirty: bool,
    ) -> Result<task::Poll<()>, <Self::Backend as ready::Backend<C>>::Failure>
    where
        C::Context: 'a,
    {
        if dirty {
            unsafe { self.poll(backend, Some(time::Duration::ZERO)) }?;
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
        let poll =
            backend.has_pending() || busy || (self.quota.remaining() == 0 && backend.can_resume());
        let span = if poll {
            Some(time::Duration::ZERO)
        } else {
            span
        };
        unsafe { self.poll(backend, span) }
    }
}
