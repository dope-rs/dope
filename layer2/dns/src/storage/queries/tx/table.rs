use std::mem;

use o3::{
    alloc,
    depot::{
        capacity,
        index::{Domain, set},
        map::direct,
    },
};
use wire::tx::Id;

use crate::{
    config,
    lookup::error,
    net::entropy,
    storage::queries::{
        index,
        tx::{self, cookies, lease},
    },
    wire::{self, cookies::Server},
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct Epoch(u64);

pub(in crate::storage::queries) struct Table<'s> {
    pool: &'s set::State<Id, u16, 2, set::Disjoint>,
    epochs: [set::Id<2>; 2],
    udp: direct::Table<Id, index::Branch>,
    tcp: direct::Table<Id, index::Branch>,
    cursor: u64,
    epoch: Epoch,
    cookies: cookies::Cookies,
}

impl<'s> Table<'s> {
    pub(in crate::storage::queries) fn try_new(
        bound: Domain<Id, u16>,
        pool: &'s set::State<Id, u16, 2, set::Disjoint>,
    ) -> Result<Self, alloc::Error> {
        let capacity = capacity::Count::new(bound.capacity() as u32);
        Ok(Self {
            pool,
            epochs: [set::Id::at::<0>(), set::Id::at::<1>()],
            udp: direct::Table::try_new(capacity)?,
            tcp: direct::Table::try_new(capacity)?,
            cursor: 0,
            epoch: Epoch::ZERO,
            cookies: cookies::Cookies::new(),
        })
    }

    pub(in crate::storage::queries) fn bind_udp(&mut self) {
        self.cookies.clear();
    }

    /// Restores a refused identity lease before reporting the allocation failure.
    pub(in crate::storage::queries) fn try_udp(
        &mut self,
        owner: index::Branch,
        server: index::Server,
        random: &entropy::Datagram<'_>,
    ) -> Result<lease::Udp<'s>, error::Identity> {
        let next = self.cursor.wrapping_add(1);
        let start = random.start(self.cursor);
        let removed = match self.pool.take_from(self.epochs[0], start.ordinal()) {
            Some(removed) => removed,
            None => {
                let removed = self.pool.take_from(self.epochs[1], start.ordinal()).ok_or(
                    error::Identity::Datagram {
                        capacity: self.udp.capacity(),
                    },
                )?;
                if let Err(error) = self.try_rotate() {
                    removed.restore();
                    return Err(error);
                }
                removed
            }
        };
        let cookie = match self.cookies.issue(server, random) {
            Ok(cookie) => cookie,
            Err(error) => {
                removed.restore();
                return Err(error);
            }
        };
        let id = Id::from_ordinal(removed.index());
        self.cursor = next;
        let capacity = self.udp.capacity();
        let Some(slot) = self.udp.vacant(id) else {
            removed.restore();
            return Err(error::Identity::Datagram { capacity });
        };
        slot.insert(owner);
        Ok(lease::Udp::new(removed, self.epoch, cookie))
    }

    pub(in crate::storage::queries) fn cookies(&self, server: index::Server) -> Option<&Server> {
        self.cookies.active(server)
    }

    pub(in crate::storage::queries) fn try_tcp(
        &mut self,
        owner: index::Branch,
        id: Id,
    ) -> Result<lease::Tcp, error::Identity> {
        let Some(slot) = self.tcp.vacant(id) else {
            return Err(error::Identity::Stream { id: id.wire() });
        };
        slot.insert(owner);
        Ok(lease::Tcp::new())
    }

    pub(in crate::storage::queries) fn release(&mut self, lease: tx::Lease<'s>) {
        match lease {
            tx::Lease::Tcp {
                id,
                capacity: tx::Tcp,
            } => {
                if let Some(slot) = self.tcp.entry(id) {
                    slot.discard();
                }
            }
            tx::Lease::Udp { removed, epoch, .. } => {
                let id = Id::from_ordinal(removed.index());
                if let Some(slot) = self.udp.entry(id) {
                    slot.discard();
                }
                let free = if epoch == self.epoch {
                    self.epochs[1]
                } else {
                    self.epochs[0]
                };
                removed.insert(free);
            }
        }
    }

    pub(in crate::storage::queries) fn cancel(&mut self, cancel: tx::Cancel) {
        let id = cancel.id();
        if let Some(slot) = self.tcp.entry(id) {
            slot.discard();
        }
    }

    pub(in crate::storage::queries) fn udp_owner(&self, id: Id) -> Option<index::Branch> {
        self.udp.get(id).copied()
    }

    pub(in crate::storage::queries) fn stream_owner(&self, id: Id) -> Option<index::Branch> {
        self.tcp.get(id).copied()
    }

    pub(in crate::storage::queries) const fn has_stream(&self) -> bool {
        !self.tcp.is_empty()
    }

    pub(in crate::storage::queries) fn cache(
        &mut self,
        cookie: tx::Cookie,
        server: index::Server,
        value: Server,
    ) -> bool {
        self.cookies.cache(cookie, server, value)
    }

    fn try_rotate(&mut self) -> Result<(), error::Identity> {
        let next = self.epoch.try_next()?;
        self.epoch = next;
        self.cookies.clear();
        self.epochs.swap(0, 1);
        Ok(())
    }
}

impl Epoch {
    const MAX: u64 = u64::MAX / config::MAX_SERVERS as u64;
    const OFFSETS: [u64; config::MAX_SERVERS] = [0, 1, 2, 3];
    pub(in crate::storage::queries::tx) const ZERO: Self = Self(0);

    pub(in crate::storage::queries::tx) fn cookie(self, server: index::Server) -> u64 {
        self.0 * config::MAX_SERVERS as u64 + server.get(&Self::OFFSETS)
    }

    pub(in crate::storage::queries::tx) fn try_next(self) -> Result<Self, error::Identity> {
        let next = self.0 + 1;
        if next > Self::MAX {
            return Err(error::Identity::Cookie {
                epoch: next,
                servers: config::MAX_SERVERS,
            });
        }
        Ok(Self(next))
    }
}

const _: () = {
    assert!(mem::size_of::<Result<(), error::Identity>>() == 24);
    assert!(mem::size_of::<lease::Udp<'static>>() == 40);
    assert!(
        mem::size_of::<Result<lease::Udp<'static>, error::Identity>>()
            == mem::size_of::<lease::Udp<'static>>()
    );
    assert!(mem::size_of::<Epoch>() == mem::size_of::<u64>());
    assert!(u16::MAX as u32 + 1 < u32::MAX);
};
