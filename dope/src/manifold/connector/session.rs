use o3::buffer::Shared;

use super::Session;
use crate::backend;
use crate::transport::link::{Establish, Outbound, PendingFlags, egress};

pub const IOV_CAP: usize = 32;

pub use egress::{Queue, Stage};

pub struct Ctx<'a, N: Session> {
    pub conn_id: backend::token::Token,
    pub state: &'a mut N::ConnState,
    pub sink: &'a mut egress::Queue<IOV_CAP>,
}

pub struct State<C: Default> {
    pub conn: C,
    pub(super) egress: egress::Queue<IOV_CAP>,
    pub(super) upstream_tag: u32,
    pub(super) pending: PendingFlags,
    pub(super) establish: Establish,
}

impl<C: Default> Default for State<C> {
    fn default() -> Self {
        Self {
            conn: C::default(),
            egress: egress::Queue::new(),
            upstream_tag: 0,
            pending: PendingFlags::default(),
            establish: Establish::Idle,
        }
    }
}

impl<C: Default> Outbound for State<C> {
    fn establish(&mut self) -> &mut Establish {
        &mut self.establish
    }
}

impl<C: Default> State<C> {
    #[must_use = "false = egress cap hit, the bytes were dropped"]
    pub fn enqueue(&mut self, bytes: Shared) -> bool {
        if self.egress.over_cap() {
            return false;
        }
        self.egress.push(bytes);
        true
    }

    #[must_use = "false = egress cap hit, nothing was enqueued"]
    pub fn enqueue_all(&mut self, frames: &[Shared]) -> bool {
        let total: usize = frames.iter().map(|f| f.len()).sum();
        if !self.egress.has_room(frames.len(), total) {
            return false;
        }
        for f in frames {
            self.egress.push(f.clone());
        }
        true
    }

    pub fn wire_stage(&mut self) -> egress::Stage<'_> {
        self.egress.wire_stage()
    }

    pub fn wire_commit(&mut self, n: usize) {
        self.egress.wire_commit(n);
    }

    pub fn egress_len(&self) -> usize {
        self.egress.total_bytes()
    }

    pub(super) fn prepare_send(
        &mut self,
        bytes_cap: usize,
    ) -> crate::transport::wire::Vectored<'_> {
        self.egress.prepare_send(bytes_cap)
    }

    pub(super) fn ack_send(&mut self, n: usize) {
        self.egress.ack(n);
    }
}
