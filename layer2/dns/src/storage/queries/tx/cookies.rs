use crate::{
    config,
    lookup::error,
    net::entropy,
    storage::queries::{index, tx::Epoch},
    wire::cookies,
};

pub(in crate::storage::queries::tx) struct Cookies {
    generation: Epoch,
    values: [Entry; config::MAX_SERVERS],
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

#[derive(Clone, Copy)]
struct Entry {
    /// Cleared generations cannot repopulate the cache.
    retired: Epoch,
    pair: Option<Pair>,
}

impl Cookies {
    pub(in crate::storage::queries::tx) const fn new() -> Self {
        Self {
            generation: Epoch::ZERO,
            values: [Entry {
                retired: Epoch::ZERO,
                pair: None,
            }; config::MAX_SERVERS],
        }
    }

    /// Unassociated requests retain distinct, checked permutation inputs.
    pub(in crate::storage::queries::tx) fn issue(
        &mut self,
        server: index::Server,
        random: &entropy::Datagram<'_>,
    ) -> Result<Cookie, error::Identity> {
        if let Some(pair) = &server.get(&self.values).pair {
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
        for value in &mut self.values {
            value.pair = None;
            value.retired = self.generation;
        }
    }

    pub(in crate::storage::queries::tx) fn active(
        &self,
        server: index::Server,
    ) -> Option<&cookies::Server> {
        Some(&server.get(&self.values).pair.as_ref()?.server)
    }

    pub(in crate::storage::queries::tx) fn cache(
        &mut self,
        cookie: Cookie,
        server: index::Server,
        value: cookies::Server,
    ) -> bool {
        let entry = server.get_mut(&mut self.values);
        if cookie.generation <= entry.retired {
            return false;
        }
        entry.pair = Some(Pair {
            cookie,
            server: value,
        });
        true
    }
}
