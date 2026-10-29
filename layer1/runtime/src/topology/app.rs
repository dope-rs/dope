use std::time;

use dope_core::driver::storage;

use crate::{
    executor::{self, session},
    topology::lending,
};

/// Exact installed application paired with the session that owns its driver.
pub struct Application<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, A, Q: 'd>
where
    S: storage::Plan + 'd,
{
    pub(super) session: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    pub(super) app: A,
}

impl<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, A, Q> Application<'scope, 'life, 'p, 'd, S, A, Q>
where
    S: storage::Plan + 'd,
    A: executor::raw::App<'d>,
    Q: executor::Startup + 'd,
{
    /// Closes the installed application through its owning session.
    pub fn finish<'report>(
        self,
        report: &'report mut session::Completion,
    ) -> Result<(), session::Error<'report, ()>> {
        self.session.finish(report, self.app)
    }

    pub fn with_app<'report, R>(
        mut self,
        report: &'report mut session::Completion,
        run: impl for<'app> FnOnce(session::App<'app, 'life, 'd, A, Q>) -> R,
    ) -> session::Result<'report, R> {
        self.session.with_app(report, self.app, run)
    }

    pub fn with_lent<'report, B>(
        self,
        report: &'report mut session::Completion,
        build: B,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        A: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, A, Q>,
    {
        session::Lent::new(self.session.core).run(report, self.app, build)
    }

    pub fn with_quiesce<'report, R>(
        mut self,
        report: &'report mut session::Completion,
        timeout: time::Duration,
        run: impl for<'app> FnOnce(session::App<'app, 'life, 'd, A, Q>) -> R,
    ) -> session::Result<'report, R> {
        self.session.with_quiesce(report, self.app, timeout, run)
    }
}
