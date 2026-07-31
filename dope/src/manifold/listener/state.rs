use std::mem::needs_drop;
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};

use super::egress::SlotFlow;
use super::send;
use super::send::Buf;
use dope_core::driver::token::SlotIndex;
use dope_net::link::egress::arena::Arena as EgressArena;
use dope_net::link::egress::queue::Queue;
use dope_net::link::slot::{DeferredEgress, PendingFlags, SendBuffer, Slot};
use dope_net::wire::Wire;

pub const WRITE_BUF_CAP: usize = send::WRITE_BUF_CAP;
type EgressQueue<'a, 'pool> = Queue<'a, 'pool, 32, SendBuffer>;

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
    bufs: Vec<Option<Box<Buf>>>,
}

impl Arena {
    fn new(hard_cap: usize) -> Self {
        let mut bufs = Vec::new();
        bufs.resize_with(hard_cap, || None);
        Self { bufs }
    }

    fn slice(&mut self, idx: SlotIndex) -> &mut [u8] {
        self.bufs[idx.raw() as usize]
            .get_or_insert_with(|| Box::new(Buf::default()))
            .as_mut_slice()
    }
}

pub(super) struct Aux {
    arena: Arena,
    scratch: Box<[u8]>,
}

/// Couples the output queue and write arena for one application callback.
pub struct EgressCtx<'a, 'pool> {
    aux: &'a mut Aux,
    queue: EgressQueue<'a, 'pool>,
}

pub struct WriteBuf<'a, 'pool> {
    pub(super) bytes: &'a mut [u8],
    pub(super) egress: EgressQueue<'a, 'pool>,
}

const _: () = assert!(
    size_of::<EgressCtx<'static, 'static>>()
        == size_of::<&'static mut Aux>() + size_of::<EgressQueue<'static, 'static>>()
);
const _: () = assert!(
    size_of::<WriteBuf<'static, 'static>>()
        == size_of::<&'static mut [u8]>() + size_of::<EgressQueue<'static, 'static>>()
);
const _: () = assert!(!needs_drop::<EgressCtx<'static, 'static>>());
const _: () = assert!(!needs_drop::<WriteBuf<'static, 'static>>());

impl Deref for WriteBuf<'_, '_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.bytes
    }
}

impl DerefMut for WriteBuf<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes
    }
}

impl<'a, 'pool> EgressCtx<'a, 'pool> {
    fn new(aux: &'a mut Aux, queue: EgressQueue<'a, 'pool>) -> Self {
        Self { aux, queue }
    }

    pub(super) fn for_slot(
        aux: &'a mut Aux,
        arena: &'a mut EgressArena<'pool, SendBuffer>,
        slot: SlotIndex,
    ) -> Self {
        Self::new(aux, arena.queue_for(slot.raw() as usize))
    }

    pub fn reborrow(&mut self) -> EgressCtx<'_, 'pool> {
        EgressCtx {
            aux: self.aux,
            queue: self.queue.reborrow(),
        }
    }

    pub fn write_buf_for<'d, W: Wire, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> WriteBuf<'_, 'pool> {
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

    pub(super) fn write_buf_raw<'d, W: Wire, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> &mut [u8] {
        self.arena.slice(slot.token().slot())
    }

    fn write_buf_for<'a, 'pool, 'd, W: Wire, C: Default + 'static>(
        &'a mut self,
        slot: &mut Slot<'d, W, State<C>>,
        egress: &'a mut EgressQueue<'_, 'pool>,
    ) -> WriteBuf<'a, 'pool> {
        let bytes = if slot.owes_egress(egress) {
            &mut self.scratch
        } else {
            self.arena.slice(slot.token().slot())
        };
        WriteBuf {
            bytes,
            egress: egress.reborrow(),
        }
    }
}
