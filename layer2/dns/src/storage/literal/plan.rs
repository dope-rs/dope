use dope::{
    core::driver::storage,
    runtime::civil::{self, Source as _},
};

use crate::{config, storage::literal};

/// Minimal storage plan for IP-literal discovery.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Plan {
    pub(crate) refresh: config::Refresh,
}

impl Plan {
    pub fn new(refresh: config::Refresh) -> Result<Self, config::Error> {
        Ok(Self {
            refresh: refresh.validate()?,
        })
    }
}

impl storage::Plan for Plan {
    type Output<'p, 'd>
        = literal::State<'d>
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Build for Plan {
    type Error = civil::Error;

    type Store<'d>
        = ()
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn try_build<'p, 'd>(
        self,
        _: &'p mut Self::Store<'d>,
        context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let now = context.clock().now();
        let clock = civil::System::NEW.anchor(&now)?;
        Ok(literal::State::new(self, clock))
    }
}

impl storage::Claimless for Plan {}
