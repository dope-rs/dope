use std::{pin, time};

use dope_core::{
    Errno,
    driver::schedule::clock::{Deadline, Sample},
};
use o3::mem::slot;

use crate::{
    client,
    executor::{
        self,
        drive::{self, clock},
        session::{self, Manual, run},
    },
    topology::lending,
};

/// Exclusive construction and execution authority for one client-bearing tree.
pub(crate) struct Lent<'scope, 'life: 'scope, 'd: 'life, Q> {
    core: &'scope mut session::Core<'life, 'd, Q>,
}

impl<'scope, 'life: 'scope, 'd: 'life, Q> Lent<'scope, 'life, 'd, Q> {
    pub(crate) fn new(core: &'scope mut session::Core<'life, 'd, Q>) -> Self {
        Self { core }
    }

    /// Issues a client only while this scope owns both construction and execution.
    pub(crate) fn run<'report, O, B>(
        self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        self.run_wall(report, owner, build, None)
    }

    pub(crate) fn run_for<'report, O, B>(
        self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
        timeout: time::Duration,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        self.run_wall(report, owner, build, Some(timeout))
    }

    fn run_wall<'report, O, B>(
        self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
        timeout: Option<time::Duration>,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        let start = self
            .core
            .admit(None)
            .map_err(|error| lending::Error::Run(session::Error::Drive(error)))?;
        let mut owner = pin::pin!(owner);
        self.pinned(report, owner.as_mut(), build, start, timeout)
    }

    pub(crate) fn held<'report, O, B>(
        self,
        report: &'report mut session::Completion,
        owner: slot::Occupied<'_, O>,
        build: B,
        timeout: Option<time::Duration>,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        let start = self
            .core
            .admit(None)
            .map_err(|error| lending::Error::Run(session::Error::Drive(error)))?;
        owner.with_pin(move |owner| self.pinned(report, owner, build, start, timeout))
    }

    fn pinned<'report, O, B>(
        mut self,
        report: &'report mut session::Completion,
        mut owner: pin::Pin<&mut O>,
        build: B,
        start: Sample,
        timeout: Option<time::Duration>,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        use drive::clock::{Bound, wall};

        let provider = lending::Root::provider(owner.as_ref());
        let client = client::Provider::provide(client::Context::new(provider));
        let (app, data) = build
            .build(client, self.core.scope.context())
            .map_err(lending::Error::Build)?;
        let result = match timeout {
            Some(timeout) => {
                let (parent, outcome, disarm) = self.execute(
                    owner.as_mut(),
                    app,
                    start,
                    |scope| {
                        let output = B::run(data, scope.app());
                        (output, Bound::for_duration(wall(), timeout))
                    },
                    drive::quiesce,
                );
                run::finish_parent(self.core, parent, report, outcome, disarm, drive::quiesce)
            }
            None => {
                let (parent, outcome, disarm) = self.execute(
                    owner.as_mut(),
                    app,
                    start,
                    |scope| (B::run(data, scope.app()), wall()),
                    drive::tick,
                );
                run::finish_parent(self.core, parent, report, outcome, disarm, drive::tick)
            }
        };
        result.map_err(lending::Error::Run)
    }

    pub(crate) fn run_at<'report, O, B>(
        mut self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
        start: Deadline<'d>,
        timeout: time::Duration,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::ExecuteAt<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        use drive::clock::{Bound, at};

        let start = self
            .core
            .admit(Some(start))
            .map_err(|error| lending::Error::Run(session::Error::Drive(error)))?;
        let mut owner = pin::pin!(owner);
        let provider = lending::Root::provider(owner.as_ref());
        let client = client::Provider::provide(client::Context::new(provider));
        let (app, data) = build
            .build(client, self.core.scope.context())
            .map_err(lending::Error::Build)?;
        let (parent, outcome, disarm) = self.execute(
            owner.as_mut(),
            app,
            start,
            |scope| {
                let output = B::run(
                    data,
                    Manual {
                        access: session::Access::new(&mut *scope.core, scope.owner.view()),
                    },
                );
                let now = scope.core.scope.context().driver_ref().clock().now().into();
                (output, Bound::for_duration(at(now), timeout))
            },
            drive::quiesce,
        );
        run::finish_parent(self.core, parent, report, outcome, disarm, drive::quiesce)
            .map_err(lending::Error::Run)
    }

    /// Ends callback-local state before consuming its exact provider continuation.
    pub(crate) fn scope_held<'report, O, E, R>(
        self,
        report: &'report mut session::Completion,
        owner: slot::Occupied<'_, O>,
        build: impl for<'issued> FnOnce(
            lending::Issued<'issued, 'life, 'd, O, Q>,
        ) -> lending::Outcome<'issued, 'life, 'd, O, Q, E, R>,
    ) -> lending::Result<'report, E, R>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        Q: executor::Startup,
    {
        let start = self
            .core
            .admit(None)
            .map_err(|error| lending::Error::Run(session::Error::Drive(error)))?;
        owner.with_pin(move |owner| {
            let provider = lending::Root::provider(owner.as_ref());
            let client = client::Provider::provide(client::Context::new(provider));
            let permit = lending::Permit::new(self.core, owner, start);
            build(lending::Issued::new(client, permit)).finish(report)
        })
    }

    pub(crate) fn execute<'parent, H, T, R, C>(
        &mut self,
        owner: pin::Pin<&'parent mut H>,
        app: T,
        start: Sample,
        run: impl for<'app> FnOnce(
            &mut run::Run<'app, 'life, 'd, lending::raw::Bound<'parent, 'd, H, T>, Q>,
        ) -> (R, C),
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> (
        executor::raw::Parent<'parent, 'd, H>,
        run::Outcome<R, C>,
        Option<Errno>,
    )
    where
        'd: 'parent,
        H: lending::raw::Resume<'d>,
        T: executor::raw::App<'d>,
        C: clock::Time,
        Q: executor::Startup,
    {
        let app = lending::raw::Bound::new(owner, app);
        let backing = pin::pin!(executor::raw::Backing::new(self.core.scope.token(), app));
        let owner = self.core.scope.with_turn(|token, driver, mut turn| {
            turn.sample(start);
            executor::raw::Owner::install(backing.as_ref().pin(), token, driver.driver_ref())
        });
        let mut scope = run::Run {
            core: &mut *self.core,
            owner,
        };
        if let Err(error) = scope.admit() {
            let (parent, disarm) = scope.cancel_lent();
            return (parent, Err(error), disarm);
        }
        let (output, source) = run(&mut scope);
        let (parent, result, disarm) = scope.finish_lent(source, tick);
        (parent, Ok((output, source, result)), disarm)
    }
}
