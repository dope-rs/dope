use std::marker;

use dope_core::driver;

use crate::{client, topology::lending};

/// Produces a client from the exact selected provider.
/// The provider type carries its storage loans; each client requires those loans
/// and its driver domain to outlive the issued application scope.
pub trait Provider<'d> {
    type Client<'app>: 'app
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app;
}

/// Owns two providers and issues their clients in the same lending scope.
/// Both pinned roots remain owned until their dependent application is released.
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = crate, runtime = crate, driver = 'd)]
pub struct Pair<'d, A: lending::Root<'d>, B: lending::Root<'d>> {
    #[dispatcher(state)]
    scope: marker::PhantomData<(
        driver::Reference<'d>,
        *const A::Provider,
        *const B::Provider,
    )>,
    #[pin]
    #[app]
    first: A,
    #[pin]
    #[app]
    second: B,
}

impl<'d, A: lending::Root<'d>, B: lending::Root<'d>> Pair<'d, A, B> {
    pub const fn new(first: A, second: B) -> Self {
        Self {
            scope: marker::PhantomData,
            first,
            second,
        }
    }
}

impl<'d, A: lending::Root<'d>, B: lending::Root<'d>> Provider<'d> for Pair<'d, A, B> {
    type Client<'app>
        = (
        <A::Provider as Provider<'d>>::Client<'app>,
        <B::Provider as Provider<'d>>::Client<'app>,
    )
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (pair, _) = context.into_parts();
        let pair = pair.project_ref();
        (
            Provider::provide(client::Context::new(lending::Root::provider(pair.first))),
            Provider::provide(client::Context::new(lending::Root::provider(pair.second))),
        )
    }
}
