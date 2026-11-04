use std::net;

use crate::{host, storage::queries};

pub(super) enum Source<'storage, 's, 'd, const LANES: usize, const ADDRESSES: usize> {
    Literal(net::SocketAddr),
    Query {
        name: Box<host::Name>,
        lane: queries::Lane<'storage, 's, 'd, LANES, ADDRESSES>,
    },
}
