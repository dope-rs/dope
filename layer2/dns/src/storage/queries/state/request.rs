use std::{mem, net, num, ops, time};

use crate::{
    config, host,
    lookup::error,
    query::answer::reply,
    storage::queries::{index, stream},
};

pub(in crate::storage::queries) struct Request<'s> {
    name: host::Name,
    valid_until: Option<time::Instant>,
    hops: reply::Hops,
    attempt: Probe,
    server: config::Selected<'s>,
    stream: Option<stream::Lease>,
    cookie_retry: bool,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::storage::queries::state) struct Probe(u8);

impl Probe {
    const ZERO: Self = Self(0);

    pub(super) const fn next(self, attempts: num::NonZero<u8>) -> Option<Self> {
        let last = attempts.get() - 1;
        if self.0 >= last {
            return None;
        }
        let next = self.0 + 1;
        Some(Self(next))
    }
}

impl<'s> Request<'s> {
    pub(in crate::storage::queries::state) fn new(
        name: host::Name,
        server: config::Selected<'s>,
    ) -> Self {
        Self {
            name,
            valid_until: None,
            hops: reply::Hops::ZERO,
            attempt: Probe::ZERO,
            server,
            stream: None,
            cookie_retry: false,
        }
    }

    pub(in crate::storage::queries) const fn name(&self) -> &host::Name {
        &self.name
    }

    pub(in crate::storage::queries) const fn server(&self) -> index::Server {
        self.server.index()
    }

    pub(in crate::storage::queries) const fn address(&self) -> net::SocketAddr {
        self.server.address()
    }

    pub(in crate::storage::queries) const fn stream(&self) -> Option<index::Server> {
        match &self.stream {
            Some(lease) => Some(lease.server()),
            None => None,
        }
    }

    pub(in crate::storage::queries::state) const fn attempt(&self) -> Probe {
        self.attempt
    }

    pub(in crate::storage::queries::state) fn retry_cookie(&mut self) -> ops::ControlFlow<()> {
        if mem::replace(&mut self.cookie_retry, true) {
            ops::ControlFlow::Break(())
        } else {
            ops::ControlFlow::Continue(())
        }
    }

    pub(in crate::storage::queries::state) fn try_expiry(
        &self,
        received: time::Instant,
        ttl: u32,
    ) -> Result<time::Instant, error::Timing> {
        let ttl = time::Duration::from_secs(u64::from(ttl));
        let until = received
            .checked_add(ttl)
            .ok_or(error::Timing::Expiry { received, ttl })?;
        Ok(match self.valid_until {
            Some(prior) => until.min(prior),
            None => until,
        })
    }

    pub(in crate::storage::queries::state) fn hops(
        &self,
        hops: reply::Hops,
    ) -> Result<reply::Hops, error::Malformed> {
        self.hops
            .add(hops)
            .map_err(|actual| error::Malformed::AliasHops {
                actual,
                maximum: reply::MAX_ALIAS_HOPS,
            })
    }

    pub(in crate::storage::queries::state) fn retry(
        mut self,
        attempt: Probe,
        live: &mut stream::Live,
    ) -> Self {
        self.attempt = attempt;
        self.cookie_retry = false;
        self.server = self.server.next();
        self.stream = match self.stream {
            None => None,
            Some(lease) => Some(live.transfer(lease, self.server.index())),
        };
        self
    }

    pub(in crate::storage::queries::state) fn try_alias(
        &mut self,
        name: host::Name,
        until: time::Instant,
        hops: reply::Hops,
    ) -> Result<(), error::Malformed> {
        let total = self.hops(hops)?;
        self.name = name;
        self.valid_until = Some(until);
        self.hops = total;
        self.attempt = Probe::ZERO;
        self.cookie_retry = false;
        Ok(())
    }

    pub(in crate::storage::queries::state) fn start_stream(
        &mut self,
        live: &mut stream::Live,
    ) -> ops::ControlFlow<()> {
        if self.stream.is_some() {
            return ops::ControlFlow::Break(());
        }
        self.stream = Some(live.acquire(self.server.index()));
        ops::ControlFlow::Continue(())
    }

    pub(in crate::storage::queries::state) fn release(self, live: &mut stream::Live) {
        if let Some(lease) = self.stream {
            live.release(lease);
        }
    }
}

const _: () = assert!(mem::size_of::<Request<'static>>() == 296);
