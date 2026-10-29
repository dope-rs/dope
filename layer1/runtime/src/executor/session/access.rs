use std::{ops::ControlFlow, pin, time};

use dope_core::driver::schedule::clock::Sample;
use drive::{block, clock, serve, turn};
use o3::base::{cell::region, time::Positive};
use turn::scope;

use crate::{
    client,
    executor::{self, drain, drive, root, session},
    topology::lending,
};

/// Borrowed capabilities shared by every live application view.
pub struct Access<
    'app,
    'life: 'app,
    'd: 'life,
    D,
    Q = (),
    Plan = <D as executor::raw::Layout<'d>>::ParkPlan,
> {
    pub(super) core: &'app mut session::Core<'life, 'd, Q>,
    view: executor::raw::View<'app, 'd, D, executor::raw::Live, Plan>,
}

impl<'app, 'life: 'app, 'd: 'life, D, Q, Plan> Access<'app, 'life, 'd, D, Q, Plan> {
    pub(super) fn reborrow(&mut self) -> Access<'_, 'life, 'd, D, Q, Plan> {
        Access {
            core: &mut *self.core,
            view: self.view,
        }
    }

    pub fn state_token(&mut self) -> &mut region::Token<region::Identity<'d, 1>> {
        &mut self.core.scope.context().into_regions().1
    }

    /// Issues the client of the complete installed root.
    pub fn root_client(&mut self) -> <D::Provider as client::Provider<'d>>::Client<'app>
    where
        D: lending::Root<'d>,
        D::Provider: 'app,
    {
        self.client(|client| client)
    }

    pub fn client<P>(
        &mut self,
        select: impl for<'borrow> FnOnce(pin::Pin<&'borrow D>) -> pin::Pin<&'borrow P>,
    ) -> <P::Provider as client::Provider<'d>>::Client<'app>
    where
        P: lending::Root<'d>,
        P::Provider: 'app,
    {
        let root = select(self.view.app(self.core.scope.token()));
        let provider = lending::Root::provider(root);
        client::Provider::provide(client::Context::new(provider))
    }

    pub fn client_with<P, C>(
        &mut self,
        select: impl for<'borrow> FnOnce(pin::Pin<&'borrow D>) -> pin::Pin<&'borrow P>,
        issue: impl for<'borrow> FnOnce(client::Context<'borrow, 'app, 'd, P>) -> C,
    ) -> C {
        let provider = select(self.view.app(self.core.scope.token()));
        issue(client::Context::new(provider))
    }

    pub fn inspect<P, C>(
        &mut self,
        select: impl for<'borrow> FnOnce(pin::Pin<&'borrow D>) -> pin::Pin<&'borrow P>,
        inspect: impl for<'borrow> FnOnce(pin::Pin<&'borrow P>) -> C,
    ) -> C {
        inspect(select(self.view.app(self.core.scope.token())))
    }
}

impl<'app, 'life: 'app, 'd: 'life, D, Q, Plan> Access<'app, 'life, 'd, D, Q, Plan>
where
    D: executor::raw::App<'d, ParkPlan = Plan>,
{
    pub(super) const fn new(
        core: &'app mut session::Core<'life, 'd, Q>,
        view: executor::raw::View<'app, 'd, D, executor::raw::Live, Plan>,
    ) -> Self {
        Self { core, view }
    }

    pub(super) fn drive<R, C>(
        self,
        source: C,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<R::Output, drive::Error>
    where
        R: root::Root<'d>,
        C: clock::Time,
    {
        self.with(|state| block::Block::new(state, source)?.run(root, tick))
    }

    pub(super) fn drive_fixed<R, C>(
        self,
        source: C,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<session::Fixed<'d, R::Output>, drive::Error>
    where
        R: root::Root<'d>,
        C: clock::Time,
    {
        self.with(|state| block::Block::new(state, source)?.run_fixed(root, tick))
    }

    pub(super) fn run(self) -> Result<(), drive::Error> {
        self.with(|state| serve::run(state, clock::wall(), drive::tick))
    }

    pub(super) fn run_for(self, timeout: time::Duration) -> Result<(), drive::Error> {
        let source = clock::Bound::until(clock::wall(), timeout)?;
        self.with(|state| serve::run(state, source, drive::timeout))
    }

    pub(super) fn run_drain(mut self, timeout: Positive) -> Result<drain::Outcome, drive::Error>
    where
        D: executor::raw::Drain<'d>,
    {
        self.reborrow().run()?;
        self.drain(timeout)
    }

    pub(super) fn drive_drain<R>(
        mut self,
        timeout: Positive,
        root: R,
    ) -> Result<ControlFlow<R::Output, drain::Outcome>, drive::Error>
    where
        D: executor::raw::Drain<'d>,
        R: root::Root<'d>,
    {
        match self
            .reborrow()
            .with(|state| block::Block::new(state, clock::wall())?.run_drain(root, drive::tick))?
        {
            ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
            ControlFlow::Continue(()) => self.drain(timeout).map(ControlFlow::Continue),
        }
    }

    fn drain(self, timeout: Positive) -> Result<drain::Outcome, drive::Error>
    where
        D: executor::raw::Drain<'d>,
    {
        let source = clock::Bound::until(clock::wall(), timeout.get())?;
        self.with(|state| drain::Run::new(state, source).run())
    }

    fn with<T>(
        self,
        run: impl FnOnce(scope::Scope<'_, '_, 'app, 'd, D, executor::raw::Live>) -> T,
    ) -> T {
        let view = self.view;
        let core = &mut *self.core;
        let pending = &mut core.pending;
        core.scope.with_turn(|token, mut driver, turn| {
            run(scope::Scope::new(&mut driver, view, token, turn, pending))
        })
    }
}
