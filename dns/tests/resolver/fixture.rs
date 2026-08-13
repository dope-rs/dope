use std::{net, time};

use dope_dns::{
    config,
    query::{self, family, server},
    transport,
};

pub(crate) fn config(servers: config::Servers, attempts: u8) -> config::Config {
    config::Config {
        policy: query::Policy::new(
            transport::DatagramThenStream,
            server::Cycle,
            family::RequireAll,
        ),
        bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
        servers,
        query: config::Query {
            timeout: time::Duration::from_secs(1),
            attempts,
        },
        refresh: config::Refresh {
            minimum: time::Duration::from_millis(10),
            backoff: config::Backoff {
                base: time::Duration::from_millis(10),
                max: time::Duration::from_secs(1),
            },
            maximum_ttl: time::Duration::from_secs(60),
            before: time::Duration::from_secs(5),
        },
    }
}
