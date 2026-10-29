use std::{mem, pin, time};

use dope_core::driver::{
    self,
    schedule::{clock, sequence},
    storage,
};

use crate::{
    executor::{
        self, boot, drive,
        session::{self, App, Manual, run},
    },
    random, shutdown,
};

pub struct Scope<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd = (), Q = ()> {
    storage: pin::Pin<&'life S::Output<'p, 'd>>,
    pub(crate) core: &'scope mut session::Core<'life, 'd, Q>,
}

const fn assert_layout<'scope, 'life: 'scope, 'p: 'life, 'd: 'p>() {
    assert!(mem::size_of::<Scope<'scope, 'life, 'p, 'd>>() == mem::size_of::<usize>() * 2);
    assert!(mem::align_of::<Scope<'scope, 'life, 'p, 'd>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();

impl<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q>
    Scope<'scope, 'life, 'p, 'd, S, Q>
{
    pub(in crate::executor) fn new(
        storage: storage::Loan<'life, 'p, 'd, S>,
        core: &'scope mut session::Core<'life, 'd, Q>,
    ) -> Self {
        Self {
            storage: storage.pinned(),
            core,
        }
    }

    pub(crate) fn reborrow(&mut self) -> Scope<'_, 'life, 'p, 'd, S, Q> {
        Scope {
            storage: self.storage,
            core: &mut *self.core,
        }
    }

    /// Borrows storage while preserving its pinned address.
    pub fn pinned(&self) -> pin::Pin<&'life S::Output<'p, 'd>> {
        self.storage
    }

    pub fn storage(&self) -> &'life S::Output<'p, 'd> {
        self.storage.get_ref()
    }

    pub fn driver(&mut self) -> driver::Context<'_, 'd> {
        self.core.scope.context()
    }

    pub fn hash_state(&mut self, domain: random::Domain) -> random::HashState<'d> {
        let driver = self.core.scope.context();
        random::HashState::for_seed(driver.driver_ref().random_seed(), domain)
    }

    pub fn try_root<P: random::Purpose>(
        &mut self,
    ) -> Result<random::Root<'d, P>, sequence::Exhausted> {
        let driver = self.core.scope.context();
        random::Root::try_acquire(&driver)
    }

    pub(crate) fn finish<'report, D>(
        self,
        report: &'report mut session::Completion,
        app: D,
    ) -> Result<(), session::Error<'report, ()>>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.with_run(app, None, report, |scope, report| scope.finish(report, ()))
    }

    pub fn with_app<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        f: impl for<'app> FnOnce(App<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.reborrow()
            .with_run(app, None, report, |mut scope, report| {
                let output = f(scope.app());
                scope.finish(report, output)
            })
    }

    /// Runs to shutdown while retaining the exact request and separate drive result.
    pub fn request<'report, D>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
    ) -> session::Result<'report, Result<shutdown::Requested<Q>, drive::Error>>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.with_app(report, app, |app| session::App::run(app))
    }

    pub fn run<'report, D>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
    ) -> session::Result<'report, Result<(), drive::Error>>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.with_app(report, app, |app| app.run().map(|_| ()))
    }

    /// Bounds application quiescence after the callback returns.
    /// Use `App::drive_for` or `App::run_for` to bound execution itself.
    pub fn with_quiesce<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        timeout: time::Duration,
        f: impl for<'app> FnOnce(App<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        use drive::clock::{Bound, wall};

        self.reborrow()
            .with_run(app, None, report, |mut scope, report| {
                let output = f(scope.app());
                scope.finish_for(report, Bound::for_duration(wall(), timeout), output)
            })
    }

    /// Serves until shutdown, then bounds application quiescence.
    pub fn run_quiesce<'report, D>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        timeout: time::Duration,
    ) -> session::Result<'report, Result<(), drive::Error>>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.with_quiesce(report, app, timeout, |app| app.run().map(|_| ()))
    }

    /// Runs with caller-controlled protocol time and bounds quiescence afterward.
    /// The timeout starts after the callback; `Manual::drive_for` bounds a drive.
    /// Later wall-clock applications reject entry until wall time catches up.
    pub fn with_at_for<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        start: clock::Deadline<'d>,
        timeout: time::Duration,
        f: impl for<'app> FnOnce(Manual<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        use drive::clock::{Bound, at};

        self.reborrow()
            .with_run(app, Some(start), report, |scope, report| {
                let output = f(Manual {
                    access: session::Access::new(&mut *scope.core, scope.owner.view()),
                });
                let now = scope.core.scope.context().driver_ref().clock().now().into();
                scope.finish_for(report, Bound::for_duration(at(now), timeout), output)
            })
    }

    pub(crate) fn drive<R>(&mut self, root: R) -> Result<R::Output, drive::Error>
    where
        R: boot::Root<'d>,
    {
        use drive::{block, clock::wall, turn};
        use turn::scope;

        let core = &mut *self.core;
        let admitted = core.scope.try_boot();
        let pending = &mut core.pending;
        core.scope.with_driver(|mut driver, turn| {
            let block = scope::Boot::try_new(&mut driver, turn, pending, admitted)
                .and_then(|state| block::Block::new(state, wall()));
            block::Block::run_boot(block, root)
        })
    }

    /// Keeps the pinned application in this frame through the aborting leaf.
    fn with_run<'report, D, R>(
        self,
        app: D,
        start: Option<clock::Deadline<'d>>,
        report: &'report mut session::Completion,
        run: impl for<'app> FnOnce(
            run::Run<'app, 'life, 'd, D, Q>,
            &'report mut session::Completion,
        ) -> session::Result<'report, R>,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        let start = self.core.admit(start).map_err(session::Error::Drive)?;
        let cell = pin::pin!(executor::raw::Backing::new(self.core.scope.token(), app));
        let app = cell.as_ref().pin();
        let owner = self.core.scope.with_turn(|token, driver, mut turn| {
            turn.sample(start);
            executor::raw::Owner::install(app, token, driver.driver_ref())
        });
        let scope = run::Run {
            core: &mut *self.core,
            owner,
        }
        .publish()
        .map_err(|(cause, disarm)| session::Error::Install { cause, disarm })?;
        run(scope, report)
    }
}
