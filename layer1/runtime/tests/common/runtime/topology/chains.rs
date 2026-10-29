use std::{convert::Infallible, mem, pin};

use dope_core::driver::{
    self, settings,
    settings::{claim, errors},
};
use dope_runtime::{
    client::{Bound, Context, Provider},
    executor,
    executor::session::{self, Completion},
    topology::{lending, lending::Compose},
};
use dope_test::harness::scenario::TIMEOUT;

use crate::common::runtime::topology::Storage;

pub struct Head(pub claim::Claim);

impl settings::Resource for Head {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.0)
    }
}

impl<'report, Q: executor::Startup> lending::ComposeFrom<Storage, Tail<'report>, Q> for Head {
    type Output = (usize, usize, usize, usize);
    type Error = lending::Error<'report, Infallible, (usize, usize, usize, usize)>;

    fn compose<'scope, 'life, 'p, 'd>(
        self,
        tail: Tail<'report>,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, Storage, Q>,
    ) -> Result<Self::Output, Self::Error>
    where
        'life: 'scope,
        'p: 'life,
        'd: 'p,
        Self: 'd,
        Tail<'report>: 'd,
        Storage: 'd,
        Q: 'd,
    {
        Compose::compose(tail, Owner {}, stage)
    }
}

#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Owner {}

// SAFETY: this empty provider retains no native operations or pending cleanup.
unsafe impl<'d> lending::raw::Resume<'d> for Owner {}

pub struct ScopedClient<'app, 'd: 'app>(pub Bound<'app, 'd, Owner>);

impl<'d> Provider<'d> for Owner {
    type Client<'app>
        = ScopedClient<'app, 'd>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (_, scope) = context.into_parts();
        ScopedClient(scope)
    }
}

pub struct Tail<'report>(pub claim::Claim, pub &'report mut Completion);

impl settings::Resource for Tail<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.0)
    }
}

impl<'report, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, Q: executor::Startup>
    lending::Compose<'scope, 'life, 'p, 'd, Storage, Q, Owner> for Tail<'report>
{
    type Output = Result<
        (usize, usize, usize, usize),
        lending::Error<'report, Infallible, (usize, usize, usize, usize)>,
    >;

    fn compose(
        self,
        owner: Owner,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, Storage, Q>,
    ) -> Self::Output {
        let storage = stage.with_stage(|stage| mem::size_of_val(stage.storage()));
        stage.lend_for(self.1, owner, Create(storage), TIMEOUT)
    }
}

pub struct Create(usize);

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
pub struct Dependent<'app, 'd: 'app> {
    #[dispatcher(state)]
    client: ScopedClient<'app, 'd>,
}

impl<'d> lending::Build<'d, Owner> for Create {
    type App<'app>
        = Dependent<'app, 'd>
    where
        Self: 'app,
        Owner: 'app,
        'd: 'app;
    type Data = usize;
    type Error = Infallible;

    fn build<'app>(
        self,
        client: ScopedClient<'app, 'd>,
        _: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Owner: 'app,
        'd: 'app,
    {
        Ok((Dependent { client }, self.0))
    }
}

impl<'life, 'd: 'life, Q> lending::Execute<'life, 'd, Owner, Q> for Create {
    type Output = (usize, usize, usize, usize);

    fn run<'app, 'run>(
        storage: Self::Data,
        _: session::App<'run, 'life, 'd, lending::raw::Bound<'app, 'd, Owner, Self::App<'app>>, Q>,
    ) -> Self::Output
    where
        Self: 'app,
        Owner: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        (
            mem::size_of::<ScopedClient<'app, 'd>>(),
            mem::size_of::<pin::Pin<&'app mut Owner>>(),
            storage,
            mem::size_of::<lending::raw::Bound<'app, 'd, Owner, Self::App<'app>>>(),
        )
    }
}
