use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_net::wire::domain;
use dope_runtime::topology::recipe;

use crate::{app::env, network::listener::topology::factory};

/// Zero-retention adapter from a storage recipe to a listener factory.
pub struct Recipe<R, B> {
    recipe: R,
    binder: B,
}

impl<R, B> Recipe<R, B> {
    pub const fn new(recipe: R, binder: B) -> Self {
        Self { recipe, binder }
    }
}

impl<R, B> settings::Resource for Recipe<R, B>
where
    B: settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.binder.include_in(needs)
    }
}

impl<R, B> factory::ReceiveSpec for Recipe<R, B>
where
    B: factory::ReceiveSpec,
{
    type Input = B::Input;
}

impl<S, ID: Identity, E, R, B> factory::Application<S, ID, E> for Recipe<R, B>
where
    S: storage::Plan,
    E: env::Spec,
    E::Transport: dope_net::Listen,
    R: recipe::Build<S>,
    B: factory::Binder<S, R, ID, E>,
{
    type Application<'life, 'p, 'd>
        = B::Application<'life, 'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        E::Wire: 'd,
        Self::Input: 'd;
    type Error = dope_core::BranchError<R::Error, <B as factory::BinderError>::Error>;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Result<
        (
            Self::Application<'life, 'p, 'd>,
            domain::Endpoint<'life, E::Wire, ID>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        E::Wire: 'd,
        Self::Input: 'd,
    {
        let service =
            recipe::Build::build(self.recipe, storage).map_err(dope_core::BranchError::First)?;
        self.binder
            .bind(service)
            .map_err(dope_core::BranchError::Second)
    }
}
