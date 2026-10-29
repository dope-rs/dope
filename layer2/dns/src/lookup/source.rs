use std::net;

use crate::lookup::query;

pub(super) enum Source<'storage, 's, 'd, const LANES: usize, const ADDRESSES: usize> {
    Literal(net::SocketAddr),
    Query(query::Query<'storage, 's, 'd, LANES, ADDRESSES>),
}
