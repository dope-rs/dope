use std::{hash::BuildHasher as _, marker, mem, net, num};

use dope_core::driver::route::{Identity, KeyTag, table::Capacity};
use dope_net::link::pool;
use dope_runtime::random;
use o3::alloc::{self, BoxUninitExt as _};

use crate::{app::env::quota::PerIp, network::listener::runtime::accept};

mod table;

pub(super) struct Full;

#[repr(transparent)]
pub(in crate::network::listener) struct Admission<'d, ID: Identity>(pool::Key<'d, ID>);

pub(super) enum Quota<'d, ID: Identity> {
    Unlimited,
    Limited {
        limit: num::NonZero<u32>,
        counts: Counts<'d, ID>,
    },
}

impl<'d, ID: Identity> Quota<'d, ID> {
    pub(super) fn try_new(
        capacity: Capacity,
        policy: PerIp,
        hash: random::HashState<'d>,
    ) -> Result<Self, accept::Prepare> {
        match policy.limit() {
            Some(limit) => Ok(Self::Limited {
                limit,
                counts: Counts::try_new(capacity, hash)?,
            }),
            None => Ok(Self::Unlimited),
        }
    }

    pub(super) const fn is_limited(&self) -> bool {
        matches!(self, Self::Limited { .. })
    }

    pub(super) fn release(&mut self, admit: Admission<'d, ID>) {
        match self {
            Self::Unlimited => {}
            Self::Limited { counts, .. } => counts.release(admit),
        }
    }

    pub(super) fn admit(
        &mut self,
        key: pool::Key<'d, ID>,
        ip: net::IpAddr,
    ) -> Result<Option<Admission<'d, ID>>, Full> {
        match self {
            Self::Unlimited => Ok(None),
            Self::Limited { limit, counts } => counts.acquire(key, ip, *limit).map(Some),
        }
    }
}

pub(super) struct Counts<'d, ID: Identity> {
    table: table::Table,
    peers: Slots,
    hash: random::HashState<'d>,
    owner: marker::PhantomData<fn(pool::Key<'d, ID>) -> pool::Key<'d, ID>>,
}

impl<'d, ID: Identity> Counts<'d, ID> {
    fn try_new(capacity: Capacity, hash: random::HashState<'d>) -> Result<Self, accept::Prepare> {
        let table = table::Table::try_new(capacity)?;
        Ok(Self {
            table,
            peers: Slots::try_new(capacity).map_err(accept::Prepare::Allocation)?,
            hash,
            owner: marker::PhantomData,
        })
    }

    fn release(&mut self, admit: Admission<'d, ID>) {
        let (ip, admit) = self.peers.take(admit);
        let hash = self.hash.hash_one(ip);
        self.table.release(admit, ip, hash);
    }

    fn acquire(
        &mut self,
        key: pool::Key<'d, ID>,
        ip: net::IpAddr,
        limit: num::NonZero<u32>,
    ) -> Result<Admission<'d, ID>, Full> {
        let hash = self.hash.hash_one(ip);
        self.table.acquire(ip, limit, hash)?;
        Ok(self.peers.bind(key, ip))
    }
}

const _: () = assert!(
    mem::size_of::<Option<Admission<'static, KeyTag<0>>>>()
        == mem::size_of::<pool::Key<'static, KeyTag<0>>>()
);

pub(super) struct Slots {
    entries: Box<[mem::MaybeUninit<net::IpAddr>]>,
}

impl Slots {
    fn try_new(capacity: Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            entries: Box::try_uninit(capacity.count().get())?,
        })
    }

    fn bind<'d, ID: Identity>(
        &mut self,
        key: pool::Key<'d, ID>,
        ip: net::IpAddr,
    ) -> Admission<'d, ID> {
        // SAFETY: listener preparation gives this table and the connection
        // pool one canonical capacity; accept_with supplies a reserved key.
        unsafe { self.slot_mut(key) }.write(ip);
        Admission(key)
    }

    fn take<'d, ID: Identity>(
        &mut self,
        admit: Admission<'d, ID>,
    ) -> (net::IpAddr, Admission<'d, ID>) {
        // SAFETY: affine Admission is minted only after this exact lane is
        // initialized and is consumed by its sole release.
        let ip = unsafe { self.slot_mut(admit.0).assume_init_read() };
        (ip, admit)
    }

    unsafe fn slot_mut<'d, ID: Identity>(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> &mut mem::MaybeUninit<net::IpAddr> {
        unsafe { self.entries.get_unchecked_mut(key.index().into_usize()) }
    }
}
