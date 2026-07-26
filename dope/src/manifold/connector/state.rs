use std::time::Instant;

use o3::buffer::Shared;

use crate::manifold::connector::source::DialKey;
use dope_net::link::core::{Establish, Outbound};
use dope_net::link::slot::PendingFlags;

use dope_net::link::egress::queue::Queue;

const IOV_CAP: usize = 32;

use dope_net::link::egress::arena::Arena;
use dope_net::wire::send::Vectored;

pub struct State<C: Default, B: AsRef<[u8]> = Shared> {
    pub conn: C,
    pub(super) egress: Queue<IOV_CAP, B>,
    pub(super) dial: DialKey,
    pub(super) pending: PendingFlags,
    pub(super) establish: Establish,
    pub(super) retired: bool,
    /// Monotonic time of the last inbound bytes from the peer, stamped from the
    /// turn clock on connect and on every recv. `None` until established. Read
    /// only for established slots, by the connector's inbound-idle liveness
    /// watchdog (`Core::poll_liveness`) — the sole detector of a silently
    /// vanished / half-open peer that never surfaces a readable EOF.
    pub(super) last_recv: Option<Instant>,
    /// Set by an app-initiated `CloseKind::Permanent` request so `close_slot`
    /// kills the dial target (no redial) instead of the default recoverable
    /// `disconnect`. Every other close path leaves this false (recoverable).
    pub(super) close_permanent: bool,
}

impl<C: Default, B: AsRef<[u8]>> Outbound for State<C, B> {
    fn establish(&mut self) -> &mut Establish {
        &mut self.establish
    }
}

impl<C: Default, B: AsRef<[u8]>> State<C, B> {
    pub(super) fn new(dial: DialKey, lane: usize, arena: &Arena<B>) -> Self {
        Self {
            conn: C::default(),
            egress: arena.queue_for(lane),
            dial,
            pending: PendingFlags::default(),
            establish: Establish::Idle,
            retired: false,
            last_recv: None,
            close_permanent: false,
        }
    }

    pub(super) fn enqueue_send(&mut self, bytes: B) -> Result<(), B> {
        self.egress.try_enqueue(bytes)
    }

    pub fn egress_len(&self) -> usize {
        self.egress.total_bytes()
    }

    pub(super) fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        self.egress.prepare_send(bytes_cap)
    }

    pub(super) fn ack_send(&mut self, n: usize) {
        self.egress.ack(n);
    }
}
