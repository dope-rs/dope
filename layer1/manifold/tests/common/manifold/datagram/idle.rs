use std::net::SocketAddr;

use dope_core::{driver, driver::route::KeyTag};
use dope_manifold::{
    app::creation,
    network::datagram::{
        Endpoint,
        hook::{self, ingress},
        plan::{Config, Plan},
    },
};
use dope_test::harness::signals::Gate;

use crate::common::manifold::datagram::handler;

pub struct Idle(pub Gate);

impl handler::State for Idle {
    type Hooks = hook::Set;
    type Mode = ingress::Access;
}

impl Idle {
    pub fn bind<'d, F>(
        self,
        addr: SocketAddr,
        callback: F,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Endpoint<'d, KeyTag<0>, handler::Handler<Idle, F>>, creation::Error>
    where
        F: for<'socket, 'access, 'work, 'turn> FnMut(
                &mut Idle,
                handler::Event<'socket, 'access, 'work, 'turn, 'd>,
            ) + Unpin,
    {
        let config = Config::new(1, 0, 1).map_err(creation::Error::Config)?;
        Plan::<KeyTag<0>>::new(config).prepare(driver)?.bind(
            addr,
            handler::Handler::new(self, callback),
            driver,
        )
    }
}
