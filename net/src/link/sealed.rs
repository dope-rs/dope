use std::{marker, mem};

use dope_core::{
    driver::{
        self, flight, ops, retained,
        route::{self, table, table::entries::vacant},
    },
    io::{
        fd::handles,
        socket::{self, establishment, option},
    },
};
use o3::collections::{self, queue::fixed};

use crate::link::pool;

pub(crate) struct Receive<'a, 'd, Tag: route::Tag> {
    fd: &'a handles::Descriptor<'d>,
    target: route::Target<'d, Tag>,
}

impl<'a, 'd, Tag: route::Tag> Receive<'a, 'd, Tag> {
    pub(crate) fn new(fd: &'a handles::Descriptor<'d>, target: route::Target<'d, Tag>) -> Self {
        Self { fd, target }
    }

    pub(crate) fn submit(
        self,
        flights: &flight::Slots<'d, Tag>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let Self { fd, target } = self;
        ops::Submit::submit_recv(driver, flights, fd, target)
    }
}

pub(crate) struct Connect<'a, 'd, const ID: u8> {
    fd: handles::Descriptor<'d>,
    addr: &'a socket::Addr,
    target: route::Target<'d, route::KeyTag<ID>>,
}

impl<'a, 'd, const ID: u8> Connect<'a, 'd, ID> {
    pub(crate) fn new(
        fd: handles::Descriptor<'d>,
        addr: &'a socket::Addr,
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> Self {
        Self { fd, addr, target }
    }

    pub(crate) fn submit<'owner>(
        self,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        options: option::StreamOptions,
    ) -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>>
    where
        'd: 'owner,
    {
        let Self { fd, addr, target } = self;
        // SAFETY: the targeted establishment state is created only after its
        // exact slab-indexed address is stored in Outbound's stable boxed
        // table. The installed owner retains that table through completion or
        // quiescence, and Connect owns the descriptor structurally.
        let connect = retained::raw::Connect::new(fd, addr, target);
        unsafe { retained::raw::Owner::submit_connect(driver, flights, options, connect) }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct RearmIndex(route::SlotIndex);

unsafe impl fixed::raw::Index for RearmIndex {
    fn index(self) -> u32 {
        self.0.raw()
    }
}

pub(crate) struct Rearm<'d, const ID: u8> {
    pending: fixed::Coalescing<route::Epoch, RearmIndex>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

const _: () = assert!(
    mem::size_of::<Rearm<'static, 0>>()
        == mem::size_of::<fixed::Fifo<RearmIndex>>()
            + mem::size_of::<Box<[Option<route::Epoch>]>>()
);
const _: () = assert!(mem::size_of::<RearmIndex>() == mem::size_of::<u32>());
const _: () = assert!(mem::align_of::<RearmIndex>() == mem::align_of::<u32>());
impl<'d, const ID: u8> Rearm<'d, ID> {
    pub(crate) fn try_with_capacity(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            pending: fixed::Coalescing::try_with_index_capacity(capacity.raw())?,
            driver: marker::PhantomData,
        })
    }

    pub(in crate::link) fn key<T>(
        &self,
        keys: pool::Keyspace<'d, ID>,
        reservation: &vacant::Entry<'_, T, route::KeyTag<ID>>,
    ) -> pool::Key<'d, ID> {
        keys.bind_table(reservation.key())
    }

    pub(in crate::link) fn queue(&mut self, key: pool::Key<'d, ID>) {
        // SAFETY: Pool creates tokens only from its paired slab reservations.
        unsafe {
            fixed::raw::Coalescing::schedule_unchecked(
                &mut self.pending,
                RearmIndex(key.lane()),
                key.epoch(),
            )
        };
    }

    pub(in crate::link) fn pop_front(
        &mut self,
        keys: pool::Keyspace<'d, ID>,
    ) -> Option<pool::Key<'d, ID>> {
        let (index, epoch) = self.pending.pop_front()?;
        Some(keys.bind_parts(index.0, epoch))
    }

    pub(in crate::link) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(in crate::link) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}
