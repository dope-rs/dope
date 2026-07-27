use std::time::Instant;

use o3::buffer::Shared;

use crate::manifold::connector::source::DialKey;
use dope_net::link::egress::queue::Queue;
use dope_net::link::egress::stage::Stage;
use dope_net::link::raw::core::{Establish, Outbound};
use dope_net::link::slot::PendingFlags;

pub const IOV_CAP: usize = 32;

use dope_net::link::egress::arena::Arena;
use dope_net::wire::send::Vectored;

pub struct State<C: Default, B: AsRef<[u8]> = Shared> {
    pub conn: C,
    pub(super) egress: Queue<IOV_CAP, B>,
    pub(super) dial: DialKey,
    pub(super) pending: PendingFlags,
    pub(super) establish: Establish,
    pub(super) retired: bool,
    /// Last inbound timestamp used to detect a silent peer.
    pub(super) last_recv: Option<Instant>,
    /// Whether the application requested a non-reconnecting close.
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

    pub fn try_enqueue(&self, bytes: B) -> Result<(), B> {
        self.egress.try_enqueue(bytes)
    }

    pub(super) fn enqueue_send(&mut self, bytes: B) -> Result<(), B> {
        self.egress.try_enqueue(bytes)
    }

    pub fn wire_stage(&mut self) -> Stage<'_, B> {
        self.egress.wire_stage()
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

impl<C: Default> State<C, Shared> {
    #[must_use = "false = egress cap hit, nothing was enqueued"]
    pub fn enqueue_all(&mut self, frames: &[Shared]) -> bool {
        self.egress.try_enqueue_all(frames)
    }
}
