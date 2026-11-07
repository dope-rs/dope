use crate::{
    config,
    lookup::error,
    net::entropy,
    storage::queries::{index, tx::Epoch},
    wire::cookies,
};

pub(in crate::storage::queries::tx) struct Cookies {
    generation: Epoch,
    retired: Epoch,
    values: [Option<Pair>; config::MAX_SERVERS],
}

/// The exact expected response cookie and its cache-retirement ordering.
#[derive(Clone, Copy)]
pub(in crate::storage::queries) struct Cookie {
    client: cookies::Client,
    generation: Epoch,
}

impl Cookie {
    pub(in crate::storage::queries) const fn client(self) -> cookies::Client {
        self.client
    }
}

#[derive(Clone, Copy)]
struct Pair {
    cookie: Cookie,
    server: cookies::Server,
}

impl Cookies {
    pub(in crate::storage::queries::tx) const fn new() -> Self {
        Self {
            generation: Epoch::ZERO,
            retired: Epoch::ZERO,
            values: [None; config::MAX_SERVERS],
        }
    }

    /// Unassociated requests retain distinct, checked permutation inputs.
    pub(in crate::storage::queries::tx) fn issue(
        &mut self,
        server: index::Server,
        random: &entropy::Datagram<'_>,
    ) -> Result<Cookie, error::Identity> {
        if let Some(pair) = server.get(&self.values) {
            return Ok(pair.cookie);
        }
        let next = self.generation.try_next()?;
        let client = random.cookie(next.cookie(server));
        self.generation = next;
        Ok(Cookie {
            client,
            generation: next,
        })
    }

    pub(in crate::storage::queries::tx) fn clear(&mut self) {
        self.values.fill(None);
        self.retired = self.generation;
    }

    pub(in crate::storage::queries::tx) fn active(
        &self,
        server: index::Server,
    ) -> Option<&cookies::Server> {
        Some(&server.get(&self.values).as_ref()?.server)
    }

    pub(in crate::storage::queries::tx) fn cache(
        &mut self,
        cookie: Cookie,
        server: index::Server,
        value: cookies::Server,
    ) -> bool {
        if cookie.generation <= self.retired {
            return false;
        }
        *server.get_mut(&mut self.values) = Some(Pair {
            cookie,
            server: value,
        });
        true
    }
}
