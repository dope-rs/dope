use std::net;

use o3::collections::{self, fixed::hash};

struct Peer {
    ip: net::IpAddr,
    connections: u32,
}

pub(super) struct Table {
    inner: hash::Map<Peer>,
}

impl Table {
    pub(super) fn try_with_capacity(
        capacity: usize,
    ) -> Result<Option<Self>, collections::AllocationError> {
        let Some(plan) = hash::Plan::new(capacity) else {
            return Ok(None);
        };
        Ok(Some(Self {
            inner: hash::Map::try_from_plan(plan)?,
        }))
    }

    pub(super) fn release(&mut self, ip: net::IpAddr, hash: u64) {
        let remove = match self.inner.get_mut(hash, |count| count.ip == ip) {
            Some(count) if count.connections > 1 => {
                count.connections -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };
        if remove {
            let _ = self.inner.remove(hash, |count| count.ip == ip);
        }
    }

    pub(super) fn acquire(&mut self, ip: net::IpAddr, limit: u32, hash: u64) -> bool {
        if let Some(count) = self.inner.get_mut(hash, |count| count.ip == ip) {
            if count.connections >= limit {
                return false;
            }
            count.connections += 1;
            return true;
        }
        self.inner
            .try_insert(hash, Peer { ip, connections: 1 }, |count| count.ip == ip)
            .is_ok()
    }
}
