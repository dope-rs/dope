use dope_core::driver::route::{self, Identity, indices, table};
use input::Dispatch;
use o3::depot::capacity;

use crate::{
    link::{
        egress::{self, data, reserve::demand},
        pool::{
            self,
            recv::{self, input},
        },
        slot::types,
    },
    wire::{self, open::Runtime as _},
};

pub struct Inspect<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Inspect<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub(super) fn new(pool: &'a pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>) -> Self {
        Self { pool }
    }

    pub fn capacity(&self) -> table::Capacity {
        self.pool.prepared.slab.view().capacity()
    }

    pub fn len(&self) -> capacity::Count {
        self.pool.prepared.slab.view().len()
    }

    pub fn available(&self) -> capacity::Count {
        self.pool.prepared.slab.view().available()
    }

    pub fn has_rearm(&self) -> bool {
        self.pool.prepared.slab.pending() != 0 || self.pool.prepared.runtime.is_ready()
    }

    pub fn has_credit(&self) -> bool {
        self.pool.prepared.egress.has_work()
    }

    pub fn is_empty(&self) -> bool {
        self.pool.prepared.slab.view().is_empty()
    }

    pub fn get(&self, key: pool::Key<'d, ID>) -> Option<&'a types::Connection<'w, 'd, ID, W, S>> {
        self.pool.prepared.slab.view().get(key.parts())
    }

    pub fn egress(
        &self,
        key: pool::Key<'d, ID>,
    ) -> Option<(&'a types::Connection<'w, 'd, ID, W, S>, egress::Pending)>
    where
        B: data::raw::Payload<'d>,
    {
        let slot = self.pool.prepared.slab.view().get(key.parts())?;
        let pending = self.pool.prepared.egress.pending(key);
        Some((slot, pending))
    }

    pub fn by_target(&self, target: route::Target<'d, ID>) -> Option<pool::Key<'d, ID>> {
        let key = pool::Key::from_target(target);
        self.pool
            .prepared
            .slab
            .view()
            .contains(key.parts())
            .then_some(key)
    }

    pub fn refresh_wake(&self, key: pool::Key<'d, ID>) {
        let Some(slot) = self.get(key) else {
            return;
        };
        slot.engine
            .ready_handle()
            .set_target(key.target().dispatch());
    }

    pub fn key_at(&self, lane: indices::SlotIndex) -> Option<pool::Key<'d, ID>> {
        let keys = *self.pool.keys;
        self.pool
            .prepared
            .slab
            .view()
            .key_at(lane)
            .map(|key| keys.bind_table(key))
    }

    pub fn recv(&self, key: pool::Key<'d, ID>) -> recv::Readiness
    where
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::inspect(self.pool, key)
    }
}
