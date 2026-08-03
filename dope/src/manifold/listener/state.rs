use std::mem::needs_drop;
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

use dope_core::driver::token::SlotIndex;
use dope_net::link::egress;
use dope_net::link::egress::queue::Queue;
use dope_net::link::slot::{DeferredEgress, PendingFlags, SendBuffer, Slot};
use dope_net::wire::Wire;

use super::egress::SlotFlow;
use super::send;
use super::send::DirectFlight;

pub const WRITE_BUF_CAP: usize = send::WRITE_BUF_CAP;
type EgressQueue<'a, 'd, 'pool> = Queue<'a, 'd, 'pool, 32, SendBuffer>;

pub struct State<C: Default + 'static> {
    pub conn: C,
    pub(super) send: send::State,
    pub(super) pending: PendingFlags,
    pub(super) deferred: DeferredEgress,
    pub(super) peer_ip: Option<IpAddr>,
}

impl<C: Default + 'static> State<C> {
    pub(super) fn new(conn: C, peer_ip: Option<IpAddr>) -> Self {
        Self {
            conn,
            send: send::State::default(),
            pending: PendingFlags::default(),
            deferred: DeferredEgress::new_for(),
            peer_ip,
        }
    }

    pub fn peer_ip(&self) -> Option<IpAddr> {
        self.peer_ip
    }
}

pub(super) struct Arena {
    flights: Vec<Option<Pin<Box<DirectFlight>>>>,
}

impl Arena {
    fn new(hard_cap: usize) -> Self {
        let mut flights = Vec::new();
        flights.resize_with(hard_cap, || None);
        Self { flights }
    }

    fn flight(&mut self, idx: SlotIndex) -> Pin<&mut DirectFlight> {
        self.flights[idx.raw() as usize]
            .get_or_insert_with(|| Box::pin(DirectFlight::new()))
            .as_mut()
    }
}

pub(super) struct Aux {
    arena: Arena,
    scratch: Box<[u8]>,
}

/// Couples the output queue and write arena for one application callback.
pub struct EgressCtx<'a, 'd, 'pool> {
    aux: &'a mut Aux,
    queue: EgressQueue<'a, 'd, 'pool>,
}

pub struct WriteBuf<'a, 'd, 'pool> {
    storage: WriteStorage<'a>,
    egress: EgressQueue<'a, 'd, 'pool>,
}

pub(super) enum WriteStorage<'a> {
    Direct(Pin<&'a mut DirectFlight>),
    Scratch(&'a mut [u8]),
}

impl WriteStorage<'_> {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Direct(flight) => flight.as_ref().header(),
            Self::Scratch(bytes) => bytes,
        }
    }
}

const _: () = assert!(
    size_of::<EgressCtx<'static, 'static, 'static>>()
        == size_of::<&'static mut Aux>() + size_of::<EgressQueue<'static, 'static, 'static>>()
);
const _: () = assert!(
    size_of::<WriteBuf<'static, 'static, 'static>>()
        == size_of::<&'static mut [u8]>() + size_of::<EgressQueue<'static, 'static, 'static>>()
);
const _: () = assert!(!needs_drop::<EgressCtx<'static, 'static, 'static>>());
const _: () = assert!(!needs_drop::<WriteBuf<'static, 'static, 'static>>());

impl Deref for WriteBuf<'_, '_, '_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.storage {
            WriteStorage::Direct(flight) => flight.as_ref().header(),
            WriteStorage::Scratch(bytes) => bytes,
        }
    }
}

impl DerefMut for WriteBuf<'_, '_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.storage {
            WriteStorage::Direct(flight) => flight.as_mut().header_mut(),
            WriteStorage::Scratch(bytes) => bytes,
        }
    }
}

impl<'a, 'd, 'pool> EgressCtx<'a, 'd, 'pool> {
    fn new(aux: &'a mut Aux, queue: EgressQueue<'a, 'd, 'pool>) -> Self {
        Self { aux, queue }
    }

    pub(super) fn for_slot(
        aux: &'a mut Aux,
        arena: &'a mut egress::arena::Arena<'d, 'pool, SendBuffer>,
        slot: SlotIndex,
    ) -> Self {
        Self::new(aux, arena.queue_for(slot.raw() as usize))
    }

    pub fn reborrow(&mut self) -> EgressCtx<'_, 'd, 'pool> {
        EgressCtx {
            aux: self.aux,
            queue: self.queue.reborrow(),
        }
    }

    pub fn write_buf_for<W: Wire, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> WriteBuf<'_, 'd, 'pool> {
        self.aux.write_buf_for(slot, &mut self.queue)
    }
}

impl Aux {
    pub(super) fn new(max_connections: usize) -> Self {
        Self {
            arena: Arena::new(max_connections),
            scratch: vec![0u8; send::WRITE_BUF_CAP].into_boxed_slice(),
        }
    }

    pub(super) fn direct_flight(&mut self, slot: SlotIndex) -> Pin<&mut DirectFlight> {
        self.arena.flight(slot)
    }

    pub(super) fn clear_direct(&mut self, slot: SlotIndex) {
        self.arena.flight(slot).clear();
    }

    fn write_buf_for<'a, 'd, 'pool, W: Wire, C: Default + 'static>(
        &'a mut self,
        slot: &mut Slot<'d, W, State<C>>,
        egress: &'a mut EgressQueue<'_, 'd, 'pool>,
    ) -> WriteBuf<'a, 'd, 'pool> {
        let storage = if slot.owes_egress(egress) {
            WriteStorage::Scratch(&mut self.scratch)
        } else {
            WriteStorage::Direct(self.arena.flight(slot.token().slot()))
        };
        WriteBuf {
            storage,
            egress: egress.reborrow(),
        }
    }
}

impl<'a, 'd, 'pool> WriteBuf<'a, 'd, 'pool> {
    pub(super) fn into_parts(self) -> (WriteStorage<'a>, EgressQueue<'a, 'd, 'pool>) {
        (self.storage, self.egress)
    }
}
