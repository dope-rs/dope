use dope::{
    core::driver::storage,
    runtime::civil::{self, Source as _},
};
use o3::depot::index::set;

use crate::{
    config,
    query::answer::addr,
    storage::{
        Error, State, Store,
        queries::{self, index, ledger},
    },
};

/// Fixed resolver storage plan for one application domain.
pub struct Plan<const LANES: usize, const ADDRESSES: usize> {
    layout: index::Layout,
    config: config::Validated,
}

impl<const LANES: usize, const ADDRESSES: usize> Plan<LANES, ADDRESSES> {
    pub fn new(config: config::Config) -> Result<Self, Error> {
        let () = addr::Addresses::<ADDRESSES>::VALID;
        let config = config::Validated::try_new(config).map_err(Error::Config)?;
        let layout = index::Layout::new(LANES)?;
        Ok(Self { layout, config })
    }
}

impl<const LANES: usize, const ADDRESSES: usize> storage::Plan for Plan<LANES, ADDRESSES> {
    type Output<'p, 'd>
        = State<'p, 'd, LANES, ADDRESSES>
    where
        Self: 'd,
        'd: 'p;
}

impl<const LANES: usize, const ADDRESSES: usize> storage::Build for Plan<LANES, ADDRESSES> {
    type Error = Error;

    type Store<'d>
        = Store
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        Store::try_new(self.layout, self.config).map_err(Error::Allocation)
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
        let now = ctx.clock().now();
        let clock = civil::System::NEW.anchor(&now).map_err(Error::Clock)?;
        let Self { layout, config } = self;
        store.config = config;
        let inner = ledger::Ledger::try_new(layout, store)?;
        let lanes = set::State::try_disjoint(layout.lanes(), set::Id::at::<1>())
            .map_err(Error::Allocation)?;
        let queries = queries::Queries {
            inner: ctx.state().bind(inner),
            lanes,
            config: &store.config,
        };
        Ok(State { queries, clock })
    }
}

impl<const LANES: usize, const ADDRESSES: usize> storage::Claimless for Plan<LANES, ADDRESSES> {}
