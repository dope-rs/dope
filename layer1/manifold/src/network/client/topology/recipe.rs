use dope_core::driver::{route::Identity, storage};
use dope_runtime::topology::recipe;

use crate::{app::env, client::session, network::client::topology::factory};

/// Zero-retention adapter from a storage recipe to a connector factory.
pub struct Recipe<R, B> {
    recipe: R,
    binder: B,
}

impl<R, B> Recipe<R, B> {
    pub const fn new(recipe: R, binder: B) -> Self {
        Self { recipe, binder }
    }
}

impl<S, ID: Identity, const MAX: usize, E, R, B> factory::Application<S, ID, MAX, E>
    for Recipe<R, B>
where
    S: storage::Plan,
    E: env::Spec,
    R: recipe::Build<S>,
    B: factory::Binder<S, R, ID, MAX, E>,
{
    type Input = B::Input;
    type Session<'life, 'p, 'd>
        = B::Session<'life, 'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Source<'life, 'p, 'd>
        = B::Source<'life, 'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Wire<'life, 'p, 'd>
        = B::Wire<'life, 'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd;
    type Error = dope_core::BranchError<R::Error, B::Error>;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd,
        E::Wire: 'd,
    {
        let service =
            recipe::Build::build(self.recipe, storage).map_err(dope_core::BranchError::First)?;
        self.binder
            .bind(service)
            .map_err(dope_core::BranchError::Second)
    }
}
