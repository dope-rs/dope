use std::net::IpAddr;
use std::ops::{Deref, DerefMut};

use super::egress::SlotFlow;
use super::send;
use super::send::Buf;
use dope_core::driver::token::SlotIndex;
use dope_net::link::egress;
use dope_net::link::slot::{DeferredEgress, PendingFlags, SendBuffer, Slot};
use dope_net::wire::Wire;

pub struct State<C: Default + 'static> {
    pub conn: C,
    pub(super) send: send::State,
    pub(super) pending: PendingFlags,
    pub(super) deferred: DeferredEgress,
    pub(super) peer_ip: Option<IpAddr>,
}

impl<C: Default + 'static> State<C> {
    pub(super) fn new(
        conn: C,
        peer_ip: Option<IpAddr>,
        lane: usize,
        arena: &egress::arena::Arena<SendBuffer>,
    ) -> Self {
        Self {
            conn,
            send: send::State::default(),
            pending: PendingFlags::default(),
            deferred: DeferredEgress::new_for(arena, lane),
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

pub struct Aux {
    arena: Arena,
    scratch: Box<[u8]>,
}

pub struct WriteBuf<'a> {
    bytes: &'a mut [u8],
}

impl Deref for WriteBuf<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.bytes
    }
}

impl DerefMut for WriteBuf<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes
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

    pub fn write_buf_for<'a, 'd, W: Wire, C: Default + 'static>(
        &'a mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> WriteBuf<'a> {
        let bytes = if slot.owes_egress() {
            &mut self.scratch
        } else {
            self.arena.slice(slot.token().slot())
        };
        WriteBuf { bytes }
    }
}

pub struct ConnView<'a, T> {
    pub state: &'a T,
    pub inflight: bool,
}
