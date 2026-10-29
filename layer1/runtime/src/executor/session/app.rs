use std::{ops, time};

use o3::base::time::Positive;

use crate::{
    executor::{
        self, drain,
        drive::{self, clock},
        root, session,
    },
    shutdown,
};

/// Live application view scoped to one installed session.
#[repr(transparent)]
pub struct App<'app, 'life: 'app, 'd: 'life, D: executor::raw::Layout<'d>, Q = ()> {
    pub(super) access: session::access::Access<'app, 'life, 'd, D, Q>,
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> App<'app, 'life, 'd, D, Q>
where
    D: executor::raw::App<'d>,
{
    pub fn drive<R>(&mut self, root: R) -> Result<R::Output, drive::Error>
    where
        R: root::Root<'d>,
    {
        self.access
            .reborrow()
            .drive(clock::wall(), root, drive::tick)
    }

    pub fn run(self) -> Result<shutdown::Requested<Q>, drive::Error> {
        self.access.run()?;
        Ok(shutdown::Requested::new())
    }

    /// Drives one root with a wall-clock deadline, checked between executor turns.
    /// Each root poll must return to the executor for the deadline to be observed.
    pub fn drive_for<R>(
        &mut self,
        timeout: time::Duration,
        root: R,
    ) -> Result<R::Output, drive::Error>
    where
        R: root::Root<'d>,
    {
        let source = clock::Bound::until(clock::wall(), timeout)?;
        self.access.reborrow().drive(source, root, drive::timeout)
    }

    /// Serves until shutdown or the wall-clock deadline, checked between turns.
    /// Each application transition must return to the executor.
    pub fn run_for(self, timeout: time::Duration) -> Result<shutdown::Requested<Q>, drive::Error> {
        self.access.run_for(timeout)?;
        Ok(shutdown::Requested::new())
    }

    pub fn run_drain(self, timeout: Positive) -> Result<drain::Report<Q>, drive::Error>
    where
        D: executor::raw::Drain<'d>,
    {
        let outcome = self.access.run_drain(timeout)?;
        Ok(drain::Report::new(shutdown::Requested::new(), outcome))
    }

    /// Returns a completed root, or drains after shutdown once the pending root is dropped.
    /// A root completed in the shutdown turn keeps its output; the drain timeout starts at shutdown.
    pub fn drive_drain<R>(
        self,
        timeout: Positive,
        root: R,
    ) -> Result<ops::ControlFlow<R::Output, drain::Report<Q>>, drive::Error>
    where
        D: executor::raw::Drain<'d>,
        R: root::Root<'d>,
    {
        Ok(self
            .access
            .drive_drain(timeout, root)?
            .map_continue(|outcome| drain::Report::new(shutdown::Requested::new(), outcome)))
    }
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> ops::Deref for App<'app, 'life, 'd, D, Q>
where
    D: executor::raw::Layout<'d>,
{
    type Target = session::access::Access<'app, 'life, 'd, D, Q>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'d, D: executor::raw::Layout<'d>, Q> ops::DerefMut for App<'_, '_, 'd, D, Q> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
