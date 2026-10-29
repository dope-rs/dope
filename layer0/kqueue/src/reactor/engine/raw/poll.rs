use dope_sys::{
    io::reactor::{
        complete::Queue as _,
        ready::{Backend as _, Poll as _, store::State as _},
    },
    table::registry::{Query, State as _},
};

use crate::reactor::{
    Engine,
    engine::{Admission, Budget, Context, Quota, Work, engine, read, ready, state, task, time},
};

impl<C: ready::Driver> ready::raw::Poll<C> for Engine<C>
where
    C::Context: ready::raw::Decode,
{
    unsafe fn resume<'a>(&'a mut self, budget: &mut impl Budget)
    where
        C::Context: 'a,
    {
        loop {
            if self.state.view().poll.is_failed() {
                break;
            }
            let Some(vacant) = self.pending.vacant() else {
                break;
            };
            let item = match budget.admit(|| self.state.edit().reads.pop_resume()) {
                Admission::Item(item) => item,
                Admission::Empty | Admission::Exhausted => break,
            };
            if let Some(poll) = self.state.poll(Query::Exact(item)) {
                unsafe { read::raw::Poll::resume(poll, vacant) };
            }
        }
    }

    unsafe fn wait<'a>(
        &'a mut self,
        timeout: Option<time::Duration>,
        changes: &mut impl Budget,
        done: &mut impl Budget,
    ) -> Result<(), Self::Failure>
    where
        C::Context: 'a,
    {
        self.check()?;
        let mut events = <C::Context as Context>::Poll::storage();
        let cap = done.remaining();
        let limit = changes.remaining();
        let edits = limit != 0 && self.has_changes();
        if cap == 0 && !edits {
            return Ok(());
        }
        let timeout = if cap == 0 || self.change_len() > limit {
            Some(time::Duration::ZERO)
        } else {
            timeout
        };
        let ready = {
            let parts = self.state.edit();
            parts
                .poll
                .wait(parts.files, &mut events, cap, timeout, changes)?
        };
        for event in ready {
            let Some(vacant) = self.pending.vacant() else {
                break;
            };
            match done.admit(|| Some(event)) {
                Admission::Item(event) => {
                    unsafe { state::dispatch::<C>(&mut self.state, vacant, event) };
                    self.check()?;
                }
                Admission::Empty | Admission::Exhausted => break,
            }
        }
        Ok(())
    }
}

impl<C> engine::raw::Poll for Engine<C>
where
    C: ready::Driver<Context: ready::raw::Decode>,
    <Self as ready::Backend<C>>::Failure: Copy + Eq,
{
    type Context = C::Context;
    type Error = <Self as ready::Backend<C>>::Failure;

    unsafe fn commit<'a, Q: Quota>(
        &'a mut self,
        quota: Q,
        deferred: impl FnOnce() -> bool,
    ) -> Result<task::Poll<()>, Self::Error>
    where
        C::Context: 'a,
    {
        use ready::raw::Work as _;

        let work = unsafe { Work::new(self, quota) };
        let dirty = self.has_changes();
        let progress = unsafe { work.commit(self, dirty) }?;
        let pending = deferred();
        Ok(if progress.is_pending() || pending {
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
        C::Context: 'a,
    {
        use ready::raw::Work as _;

        let work = unsafe { Work::new(self, quota) };
        unsafe { work.wait(self, timeout, busy()) }
    }
}
