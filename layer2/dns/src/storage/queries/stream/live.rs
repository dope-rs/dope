use std::mem;

use crate::{
    config,
    storage::queries::{index, stream},
};

pub(in crate::storage::queries) struct Live {
    counts: [usize; config::MAX_SERVERS],
}

impl Live {
    pub(in crate::storage::queries) const fn new() -> Self {
        Self {
            counts: [0; config::MAX_SERVERS],
        }
    }

    pub(in crate::storage::queries) fn acquire(&mut self, server: index::Server) -> stream::Lease {
        *server.get_mut(&mut self.counts) += 1;
        stream::Lease::acquired(server)
    }

    pub(in crate::storage::queries) fn transfer(
        &mut self,
        lease: stream::Lease,
        to: index::Server,
    ) -> stream::Lease {
        let from = lease.server();
        if from == to {
            return lease;
        }
        *from.get_mut(&mut self.counts) -= 1;
        *to.get_mut(&mut self.counts) += 1;
        stream::Lease::acquired(to)
    }

    pub(in crate::storage::queries) fn release(&mut self, lease: stream::Lease) {
        let server = lease.server();
        *server.get_mut(&mut self.counts) -= 1;
    }

    pub(in crate::storage::queries) fn any(&self) -> bool {
        self.counts.iter().any(|live| *live != 0)
    }

    pub(in crate::storage::queries) fn contains(&self, server: index::Server) -> bool {
        *server.get(&self.counts) != 0
    }
}

const _: () = assert!(mem::size_of::<Live>() == mem::size_of::<[usize; config::MAX_SERVERS]>());
