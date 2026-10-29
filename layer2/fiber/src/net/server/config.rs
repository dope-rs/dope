use dope::{
    core::driver::route::Identity,
    net::{
        self,
        link::{egress, pool::recv::input},
    },
};

use crate::{
    core::{driver::route::table::conn::Capacity, io::socket},
    manifold::{
        app::env,
        network::{listener, listener::config},
        timing,
    },
};

pub struct Config<T: net::Listen, R> {
    pub direct_flights: config::Flights,
    pub bind: T::Addr,
    pub backlog: socket::Backlog,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: egress::Config,
    pub bound: T::BoundListener,
    pub wire: R,
}

impl<T: net::Listen, R> Config<T, R> {
    pub(super) fn prepare<'w, W: input::Execution, ID: Identity>(
        self,
        cap: Capacity,
    ) -> listener::Install<'w, ID, env::Bundle<T, W, timing::Balanced>, R> {
        let config = config::Prepared {
            per_ip: None,
            connections: cap,
            direct_flights: self.direct_flights,
            bind: self.bind,
            backlog: self.backlog,
            stream: self.stream,
            transport: self.transport,
            egress: self.egress,
        };
        listener::Install::new(config, self.wire).with_bound(Some(self.bound))
    }
}
