use dope_core::{driver::route::table, io::fd::handles};

use crate::{link::pool, wire};

pub struct Inspection<
    'a,
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B,
    const IOV: usize,
> {
    pool: &'a pool::Connections<'d, ID, T, W, S, M, B, IOV>,
}

impl<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Inspection<'a, 'd, ID, T, W, S, M, B, IOV>
{
    pub(super) fn new(pool: &'a pool::Connections<'d, ID, T, W, S, M, B, IOV>) -> Self {
        Self { pool }
    }

    pub fn capacity(&self) -> table::Capacity {
        self.pool.prepared.slab.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.pool.prepared.slab.is_empty()
    }

    pub fn available(&self) -> usize {
        self.pool
            .prepared
            .slab
            .capacity()
            .get()
            .saturating_sub(self.pool.prepared.slab.len())
    }

    pub fn pending_rearm(&self) -> bool {
        !self.pool.prepared.scheduling.rearm.is_empty()
    }

    pub fn has_io_targets(&self) -> bool {
        !self.pool.prepared.slab.is_empty()
    }

    pub fn fd_of(&self, key: pool::Key<'d, ID>) -> Option<&'a handles::Descriptor<'d>> {
        self.pool.get(key).and_then(|slot| slot.engine.fd())
    }
}

impl<const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Inspection<'_, '_, ID, T, W, S, M, B, IOV>
{
    pub(in crate::link) fn has_outbound_targets_unchecked(&self) -> bool {
        !self.pool.prepared.slab.is_empty()
    }
}
