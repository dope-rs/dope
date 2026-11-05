use std::mem;

use o3::{
    alloc::{self, BoxSliceExt as _},
    depot::index::set,
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
    owners: Box<[Option<index::Branch>; Id::CAPACITY * 2]>,
    streams: u32,
    cursor: u64,
    epoch: Epoch,
    cookies: cookies::Cookies,
}

impl<'s> Table<'s> {
    #[inline]
    pub(in crate::storage::queries) fn try_new(
        pool: &'s set::State<Id, u16, 2, set::Disjoint>,
    ) -> Result<Self, alloc::Error> {
        let Ok(owners) = Box::try_repeat(Id::CAPACITY * 2, None)?.try_into() else {
            unreachable!("length");
        };
        Ok(Self {
            pool,
            epochs: [set::Id::at::<0>(), set::Id::at::<1>()],
            owners,
            streams: 0,
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
                        capacity: Id::CAPACITY,
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
        self.cursor = next;
        let slot = &mut self.owners[usize::from(removed.index().raw())];
        if slot.is_some() {
            removed.restore();
            return Err(error::Identity::Datagram {
                capacity: Id::CAPACITY,
            });
        }
        *slot = Some(owner);
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
        let slot = &mut self.owners[Id::CAPACITY + usize::from(id.wire())];
        if slot.is_some() {
            return Err(error::Identity::Stream { id: id.wire() });
        }
        *slot = Some(owner);
        self.streams += 1;
        Ok(lease::Tcp::new())
    }

    pub(in crate::storage::queries) fn release(&mut self, lease: tx::Lease<'s>) {
        match lease {
            tx::Lease::Tcp {
                id,
                capacity: tx::Tcp,
            } => self.clear_tcp(id),
            tx::Lease::Udp { removed, epoch, .. } => {
                self.owners[usize::from(removed.index().raw())] = None;
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
        self.clear_tcp(cancel.id());
    }

    pub(in crate::storage::queries) fn udp_owner(&self, id: Id) -> Option<index::Branch> {
        self.owners[usize::from(id.wire())]
    }

    pub(in crate::storage::queries) fn stream_owner(&self, id: Id) -> Option<index::Branch> {
        self.owners[Id::CAPACITY + usize::from(id.wire())]
    }

    pub(in crate::storage::queries) const fn has_stream(&self) -> bool {
        self.streams != 0
    }

    pub(in crate::storage::queries) fn cache(
        &mut self,
        cookie: tx::Cookie,
        server: index::Server,
        value: Server,
    ) -> bool {
        self.cookies.cache(cookie, server, value)
    }

    fn clear_tcp(&mut self, id: Id) {
        if self.owners[Id::CAPACITY + usize::from(id.wire())]
            .take()
            .is_some()
        {
            self.streams -= 1;
        }
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
    assert!(Id::CAPACITY < u32::MAX as usize);
};
