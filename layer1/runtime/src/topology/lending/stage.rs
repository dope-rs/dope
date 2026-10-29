use std::{mem, time};

use dope_core::driver::{self, schedule::clock, storage};
use o3::mem::slot;

use crate::{
    executor::{self, drive, session},
    random::{self, HashState},
    shutdown,
    topology::{self, lending},
};

/// Installation authority for one borrowed-root scope.
pub struct Stage<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q> {
    pub(super) session: &'stage mut session::Scope<'scope, 'life, 'p, 'd, S, Q>,
}

impl<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q>
    Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>
{
    pub(in crate::topology) fn new(
        session: &'stage mut session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self {
        Self { session }
    }

    pub fn storage(&self) -> &'life S::Output<'p, 'd> {
        self.session.storage()
    }

    pub fn hash_state(&mut self, domain: random::Domain) -> HashState<'d> {
        self.session.hash_state(domain)
    }

    pub fn driver(&mut self) -> driver::Context<'_, 'd> {
        self.session.driver()
    }

    pub fn with_stage<R>(
        &mut self,
        install: impl FnOnce(&mut topology::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>) -> R,
    ) -> R {
        let mut stage = topology::Stage::new(self.session);
        install(&mut stage)
    }

    /// Establishes provider residence without issuing a client or installing it.
    pub fn hold<O, R>(
        &mut self,
        owner: O,
        prepare: impl for<'held> FnOnce(lending::Held<'held, 'd, O>, &mut Self) -> R,
    ) -> R
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
    {
        let mut place = mem::MaybeUninit::uninit();
        let owner = slot::Vacant::new(&mut place).insert(owner);
        prepare(lending::Held::new(owner), self)
    }

    /// Constructs and executes one dependent application in the sole client scope.
    pub fn with_lent<'report, O, B>(
        &mut self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::Execute<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        session::Lent::new(self.session.core).run(report, owner, build)
    }

    /// Bounds quiescence after dependent execution relinquishes its client scope.
    pub fn lend_for<'report, O, B>(
        &mut self,
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
        session::Lent::new(self.session.core).run_for(report, owner, build, timeout)
    }

    /// Runs one affine request while callback-local projected state stays pinned.
    pub fn lend_scope<'report, O, E, R>(
        &mut self,
        report: &'report mut session::Completion,
        owner: O,
        build: impl for<'issued> FnOnce(
            lending::Issued<'issued, 'life, 'd, O, Q>,
        ) -> lending::Outcome<'issued, 'life, 'd, O, Q, E, R>,
    ) -> lending::Result<'report, E, R>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        Q: executor::Startup,
    {
        let mut place = mem::MaybeUninit::uninit();
        let owner = slot::Vacant::new(&mut place).insert(owner);
        session::Lent::new(self.session.core).scope_held(report, owner, build)
    }

    /// Executes with caller-controlled protocol time and bounded quiescence.
    pub fn lend_at<'report, O, B>(
        &mut self,
        report: &'report mut session::Completion,
        owner: O,
        build: B,
        start: clock::Deadline<'d>,
        timeout: time::Duration,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        O: lending::Root<'d> + lending::raw::Resume<'d>,
        B: lending::ExecuteAt<'life, 'd, O, Q>,
        Q: executor::Startup,
    {
        session::Lent::new(self.session.core).run_at(report, owner, build, start, timeout)
    }

    pub fn with_app<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        run: impl for<'app> FnOnce(session::App<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.session.with_app(report, app, run)
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
        self.session.request(report, app)
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
        self.session.run(report, app)
    }

    pub fn with_quiesce<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        timeout: time::Duration,
        run: impl for<'app> FnOnce(session::App<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.session.with_quiesce(report, app, timeout, run)
    }

    pub fn with_at_for<'report, D, R>(
        &mut self,
        report: &'report mut session::Completion,
        app: D,
        start: clock::Deadline<'d>,
        timeout: time::Duration,
        run: impl for<'app> FnOnce(session::Manual<'app, 'life, 'd, D, Q>) -> R,
    ) -> session::Result<'report, R>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        self.session.with_at_for(report, app, start, timeout, run)
    }

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
        self.session.run_quiesce(report, app, timeout)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Stage<'_, '_, '_, '_, '_, (), ()>>()
            == mem::size_of::<&mut session::Scope<'_, '_, '_, '_, (), ()>>()
    );
    assert!(
        mem::align_of::<Stage<'_, '_, '_, '_, '_, (), ()>>()
            == mem::align_of::<&mut session::Scope<'_, '_, '_, '_, (), ()>>()
    );
    assert!(!mem::needs_drop::<Stage<'_, '_, '_, '_, '_, (), ()>>());
};
