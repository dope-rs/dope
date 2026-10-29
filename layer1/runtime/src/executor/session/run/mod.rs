use dope_core::{Errno, driver::schedule::clock::Sample};
use drive::{clock, turn};
use turn::scope;

use crate::{
    executor::{self, drive, session},
    topology::lending,
};

mod quiesce;

pub(crate) type Outcome<R, C> = Result<(R, C, Result<(), drive::Error>), executor::InstallError>;

pub(crate) struct Run<'app, 'life: 'app, 'd: 'life, D, Q>
where
    D: executor::raw::Layout<'d>,
{
    pub(super) core: &'app mut session::Core<'life, 'd, Q>,
    pub(super) owner: executor::raw::Owner<'app, 'd, D, executor::raw::Live>,
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> Run<'app, 'life, 'd, D, Q>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn app(&mut self) -> session::App<'_, 'life, 'd, D, Q> {
        session::App {
            access: session::Access::new(self.core, self.owner.view()),
        }
    }

    pub(super) fn publish(self) -> Result<Self, (executor::InstallError, Option<Errno>)>
    where
        Q: executor::Startup,
    {
        let mut scope = self;
        if let Err(error) = scope.admit() {
            let disarm = scope.cancel();
            return Err((error, disarm));
        }
        Ok(scope)
    }

    pub(super) fn admit(&mut self) -> Result<(), executor::InstallError>
    where
        Q: executor::Startup,
    {
        self.owner
            .admit(self.core.scope.token())
            .and_then(|()| Q::installed(self.core.lifecycle))
    }

    fn shutdown<C>(
        self,
        source: C,
    ) -> (
        &'app mut session::Core<'life, 'd, Q>,
        executor::raw::Owner<'app, 'd, D, executor::raw::Draining>,
    )
    where
        C: clock::Time,
    {
        let Self { core, owner } = self;
        let tick = source.start();
        let owner = core.scope.with_turn(|token, mut driver, mut schedule| {
            let turn = schedule.begin_at(tick);
            owner.shutdown(token, turn.turn(), driver.reborrow())
        });
        (core, owner)
    }

    fn finish_owner(
        core: &mut session::Core<'life, 'd, Q>,
        owner: executor::raw::Owner<'app, 'd, D, executor::raw::Draining>,
        forced: bool,
    ) -> Option<Errno> {
        core.pending = None;
        core.scope.finish(forced, |token, finish| {
            owner.finish(token, finish);
            finish.disarm_error()
        })
    }

    fn resolve<'report, R>(
        core: &mut session::Core<'life, 'd, Q>,
        owner: executor::raw::Owner<'app, 'd, D, executor::raw::Draining>,
        output: R,
        result: Result<(), drive::Error>,
        disarm: Option<Errno>,
        report: &'report mut session::Completion,
    ) -> session::Result<'report, R> {
        let cleanup = Self::finish_owner(core, owner, result.is_err());
        let (drive, disarm) = match (result, disarm.or(cleanup)) {
            (Ok(()), None) => return Ok(output),
            (Ok(()), Some(cause)) => (drive::Error::TimerDisarm(cause), None),
            (Err(drive), disarm) => (drive, disarm),
        };
        Err(report.failed(output, drive, disarm))
    }

    fn finish_with<'report, R, C>(
        self,
        report: &'report mut session::Completion,
        source: C,
        output: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> session::Result<'report, R>
    where
        C: clock::Time,
    {
        let (core, owner) = self.shutdown(source);
        let result = Self::quiesce(core, &owner, source, tick);
        Self::resolve(core, owner, output, result, None, report)
    }

    fn quiesce<C>(
        core: &mut session::Core<'life, 'd, Q>,
        owner: &executor::raw::Owner<'app, 'd, D, executor::raw::Draining>,
        source: C,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<(), drive::Error>
    where
        C: clock::Time,
    {
        let view = owner.view();
        let pending = &mut core.pending;
        core.scope.with_turn(|token, mut driver, schedule| {
            let state = scope::Scope::new(&mut driver, view, token, schedule, pending);
            quiesce::run(state, source, tick)
        })
    }

    pub(crate) fn finish_for<'report, R, C>(
        self,
        report: &'report mut session::Completion,
        source: C,
        output: R,
    ) -> session::Result<'report, R>
    where
        C: clock::Expiring,
    {
        self.finish_with(report, source, output, drive::quiesce)
    }

    pub(crate) fn finish<'report, R>(
        self,
        report: &'report mut session::Completion,
        output: R,
    ) -> session::Result<'report, R> {
        self.finish_with(report, clock::wall(), output, drive::tick)
    }

    pub(super) fn cancel(self) -> Option<Errno> {
        let now = self.core.scope.driver_ref().clock().now().into();
        let (core, owner) = self.shutdown(clock::at(now));
        Self::finish_owner(core, owner, true)
    }
}

impl<'app, 'parent: 'app, 'life: 'app, 'd: 'life + 'parent, H, T, Q>
    Run<'app, 'life, 'd, lending::raw::Bound<'parent, 'd, H, T>, Q>
where
    H: lending::raw::Resume<'d>,
    T: executor::raw::App<'d>,
{
    pub(super) fn finish_lent<C>(
        self,
        source: C,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> (
        executor::raw::Parent<'parent, 'd, H>,
        Result<(), drive::Error>,
        Option<Errno>,
    )
    where
        C: clock::Time,
    {
        let (core, owner) = self.shutdown(source);
        let result = Self::quiesce(core, &owner, source, tick);
        core.pending = None;
        let (parent, disarm) = core.scope.finish(result.is_err(), |token, finish| {
            let parent = owner.finish_tail(token, finish);
            (parent, finish.disarm_error())
        });
        (parent, result, disarm)
    }

    pub(super) fn cancel_lent(self) -> (executor::raw::Parent<'parent, 'd, H>, Option<Errno>) {
        let now = self.core.scope.driver_ref().clock().now().into();
        let (core, owner) = self.shutdown(clock::at(now));
        core.pending = None;
        core.scope.finish(true, |token, finish| {
            let parent = owner.finish_tail(token, finish);
            (parent, finish.disarm_error())
        })
    }
}

/// Resumes the same stopped provider after every dependent value has been destroyed.
pub(crate) fn finish_parent<'report, 'parent, 'life, 'd: 'life + 'parent, H, Q, R, C>(
    core: &mut session::Core<'life, 'd, Q>,
    parent: executor::raw::Parent<'parent, 'd, H>,
    report: &'report mut session::Completion,
    outcome: Outcome<R, C>,
    disarm: Option<Errno>,
    tick: impl Fn(C) -> Result<Sample, drive::Error>,
) -> session::Result<'report, R>
where
    H: lending::raw::Resume<'d>,
    C: clock::Time,
{
    parent.with_owner(core, |core, owner| match outcome {
        Ok((output, source, result)) => {
            let result = result.and_then(|()| Run::quiesce(core, &owner, source, tick));
            Run::resolve(core, owner, output, result, disarm, report)
        }
        Err(cause) => {
            let cleanup = Run::finish_owner(core, owner, true);
            Err(session::Error::Install {
                cause,
                disarm: disarm.or(cleanup),
            })
        }
    })
}
