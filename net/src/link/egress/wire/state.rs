use super::super::metadata::MetadataQueue;
use super::super::raw::entry::Entry;
use super::super::stage::Stage;
use super::super::{WireLease, WirePool};

pub(in crate::link::egress) struct WireState<'a, 'pool> {
    pool: &'pool WirePool,
    lease: &'a mut Option<WireLease<'pool>>,
}

impl<'a, 'pool> WireState<'a, 'pool> {
    pub(super) fn new(pool: &'pool WirePool, lease: &'a mut Option<WireLease<'pool>>) -> Self {
        Self { pool, lease }
    }

    pub(in crate::link::egress) fn stage<'stage, B>(
        &'stage mut self,
        entries: MetadataQueue<'stage, Entry<B>>,
    ) -> Stage<'stage, 'pool, B> {
        self.acquire();
        Stage::open(self.lease, entries)
    }

    fn acquire(&mut self) {
        if self.lease.is_none() {
            *self.lease = self.pool.try_acquire();
        }
    }

    pub(in crate::link::egress) fn try_consume(&mut self, amount: usize) -> bool {
        if let Some(lease) = self.lease.as_mut() {
            let Ok(prefix) = lease.try_consume_prefix(amount) else {
                return false;
            };
            prefix.commit();
            if lease.is_empty() {
                self.lease.take();
            }
            return true;
        }
        amount == 0
    }

    pub(in crate::link::egress) fn reborrow(&mut self) -> WireState<'_, 'pool> {
        WireState {
            pool: self.pool,
            lease: self.lease,
        }
    }
}
