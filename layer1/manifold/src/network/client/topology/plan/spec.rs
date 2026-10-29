use std::time;

use dope_net::link::egress;
use dope_runtime::random;
use o3::base::time::Positive;

use crate::{client::side, network::client, service::health};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Spec {
    retry: health::Plan,
    egress: egress::Config,
    connection_lifetime: Option<Positive>,
}

impl Spec {
    pub fn new(base: time::Duration, max: time::Duration) -> Result<Self, health::Error> {
        Ok(Self {
            retry: health::Plan::new(base, max)?,
            egress: egress::Config::DEFAULT,
            connection_lifetime: None,
        })
    }

    #[must_use]
    pub const fn with_egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    /// Opts this connector service into a maximum connection age.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: Positive) -> Self {
        self.connection_lifetime = Some(ttl);
        self
    }

    pub(super) fn bind<'d, W>(
        self,
        caps: side::Capacities,
        hash: random::HashState<'d>,
        wire: W,
    ) -> client::Config<'d, W> {
        client::Config::new(caps, self.retry.bind(hash), wire)
            .with_egress(self.egress)
            .with_ttl(self.connection_lifetime)
    }
}
