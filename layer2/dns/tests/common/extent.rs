use dope::core::driver::storage;
use dope_dns::storage::{Error, Plan, State, Store};

pub struct Extent {
    pub source: Plan<1, 1>,
    pub target: Plan<2, 1>,
}

impl storage::Plan for Extent {
    type Output<'p, 'd>
        = State<'p, 'd, 2, 1>
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Build for Extent {
    type Error = Error;
    type Store<'d>
        = Store
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        self.source.try_store()
    }

    fn try_build<'p, 'd>(
        self,
        store: &'p mut Self::Store<'d>,
        ctx: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        self.target.try_build(store, ctx)
    }
}
