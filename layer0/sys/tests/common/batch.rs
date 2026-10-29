use std::{array, cell::Cell, convert::Infallible, ops::ControlFlow, task, time};

use dope_sys::{
    engine,
    engine::life::{Reclaim, Resolver, Step},
    io::reactor::ready::store::Quota,
    probe::Stopped,
};

#[derive(Debug)]
pub struct Batch {
    pub(crate) entries: array::IntoIter<Result<(), u16>, 3>,
    pub(crate) reports: Vec<u16>,
    pub(crate) reads: usize,
    pub(crate) calls: Cell<usize>,
}

impl engine::raw::Poll for Batch {
    type Context = ();
    type Error = Infallible;

    unsafe fn commit<'a, Q: Quota>(
        &'a mut self,
        _quota: Q,
        _defer: impl FnOnce() -> bool,
    ) -> Result<task::Poll<()>, Self::Error>
    where
        Self::Context: 'a,
    {
        self.calls.set(self.calls.get() + 1);
        Ok(task::Poll::Ready(()))
    }

    unsafe fn wait<'a, Q: Quota>(
        &'a mut self,
        _quota: Q,
        _timeout: Option<time::Duration>,
        _busy: impl FnOnce() -> bool,
    ) -> Result<(), Self::Error>
    where
        Self::Context: 'a,
    {
        self.calls.set(self.calls.get() + 1);
        Ok(())
    }
}
impl Resolver for Batch {
    type Backend = Self;
    type Fault = u16;

    fn backend(&mut self) -> &mut Self {
        self.calls.set(self.calls.get() + 1);
        self
    }

    fn retire_one(&mut self) -> Step {
        self.calls.set(self.calls.get() + 1);
        Step::Empty
    }

    fn has_pending(&self) -> bool {
        self.calls.set(self.calls.get() + 1);
        false
    }

    fn has_flights(&self) -> bool {
        self.calls.set(self.calls.get() + 1);
        false
    }

    fn has_inflight(&self) -> bool {
        self.calls.set(self.calls.get() + 1);
        false
    }
}

impl Reclaim for Batch {
    fn reclaim(mut self) -> Result<ControlFlow<Self, Self>, Stopped<Self, u16>> {
        self.reads += 1;
        match self.entries.next() {
            Some(Ok(())) => Ok(ControlFlow::Continue(self)),
            Some(Err(cause)) => Err(Stopped { state: self, cause }),
            None => Ok(ControlFlow::Break(self)),
        }
    }

    fn fault(mut failure: Stopped<Self, u16>) -> Self {
        failure.state.reports.push(failure.cause);
        failure.state
    }

    fn reactor(&mut self, failure: Infallible) {
        match failure {}
    }
}
