use std::{mem, net};

use dope::manifold::service::{self, discover};
use o3::base::cell::region;

use crate::{
    config,
    storage::{self, queries::index},
};

pub(crate) struct Set<'life, 's, 'd: 'life, const LANES: usize, const N: usize> {
    store: &'life storage::State<'s, 'd, LANES, N>,
}

impl<'life, 's, 'd: 'life, const LANES: usize, const N: usize> Set<'life, 's, 'd, LANES, N> {
    pub(crate) const fn new(store: &'life storage::State<'s, 'd, LANES, N>) -> Self {
        Self { store }
    }

    pub(crate) fn snapshot(
        &self,
    ) -> service::Snapshot<service::Endpoint<index::Server, net::SocketAddr>, { config::MAX_SERVERS }>
    {
        let servers = &self.store.queries.config.servers;
        let peers = servers.map(service::Endpoint::new);
        service::Snapshot::from_vec(service::Revision::new(1), peers)
    }
}

impl<'life, 's, 'd: 'life, const LANES: usize, const N: usize> discover::Demand<'d>
    for Set<'life, 's, 'd, LANES, N>
{
    type Endpoint = index::Server;

    fn is_needed(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        self.store.queries.has_stream(token)
    }

    fn is_eligible(
        &self,
        server: &Self::Endpoint,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> bool {
        self.store.queries.has_pending(token, *server)
    }
}

const _: () = assert!(
    mem::size_of::<Set<'static, 'static, 'static, 1, 1>>()
        == mem::size_of::<&'static storage::State<'static, 'static, 1, 1>>()
);
