use dope_core::{
    driver::{
        self,
        route::{Identity, indices, table::conn::Capacity},
        schedule::ready,
    },
    io::socket,
};
use dope_net::link::oneshot;

use crate::{
    app::creation,
    client::side::{
        self,
        mode::{Mode, Transport},
        registry,
    },
};

pub(crate) trait Startup<'d, ID: Identity + 'd>: Mode<'d, ID> + Sized {
    fn prepare<P>(
        self,
        caps: side::Capacities,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(Self, Self::Transport<P>, Capacity), creation::Error>
    where
        P: socket::Prepared;

    fn start(self, target: ready::Target<'d>) -> Self;
}

impl<'d, ID: Identity + 'd> Startup<'d, ID> for side::Disabled {
    fn prepare<P>(
        self,
        caps: side::Capacities,
        _: &mut driver::Context<'_, 'd>,
    ) -> Result<(Self, Self::Transport<P>, Capacity), creation::Error>
    where
        P: socket::Prepared,
    {
        let capacity = caps.primary();
        Ok((self, (), capacity))
    }

    fn start(self, _: ready::Target<'d>) -> Self {
        self
    }
}

impl<'d, ID: Identity + 'd, I> Startup<'d, ID> for side::Enabled<&registry::Registry<'d, I, ID>>
where
    I: registry::Intent<'d, ID>,
{
    fn prepare<P>(
        self,
        caps: side::Capacities,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(Self, Self::Transport<P>, Capacity), creation::Error>
    where
        P: socket::Prepared,
    {
        let capacity = caps.primary();
        let total = caps.total();
        let io = oneshot::Pool::prepare(indices::Edge::from(capacity), capacity.into(), driver)?;
        Ok((self, Transport::new(io), total))
    }

    fn start(self, target: ready::Target<'d>) -> Self {
        self.registry.start(target);
        self
    }
}
