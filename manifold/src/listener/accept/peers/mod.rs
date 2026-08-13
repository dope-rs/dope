use std::{hash, net};

use o3::collections;

mod table;

pub(super) struct Counts<H> {
    table: table::Table,
    hash_builder: H,
}

impl<H: hash::BuildHasher> Counts<H> {
    pub(super) fn try_with_capacity(
        capacity: usize,
        hash_builder: H,
    ) -> Result<Option<Self>, collections::AllocationError> {
        let Some(table) = table::Table::try_with_capacity(capacity)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            table,
            hash_builder,
        }))
    }

    pub(super) fn release(&mut self, ip: net::IpAddr) {
        let hash = self.hash_builder.hash_one(ip);
        self.table.release(ip, hash);
    }

    pub(super) fn acquire(&mut self, ip: net::IpAddr, limit: u32) -> bool {
        let hash = self.hash_builder.hash_one(ip);
        self.table.acquire(ip, limit, hash)
    }
}
