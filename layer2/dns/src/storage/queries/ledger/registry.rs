use std::mem;

use crate::{
    lookup::error,
    storage::queries::{ledger, state, stream, tx},
};

pub(in crate::storage::queries) struct Registry<'s> {
    pub(in crate::storage::queries) pending: ledger::Pending<'s>,
    pub(in crate::storage::queries) live: stream::Live,
    pub(in crate::storage::queries) ids: tx::Table<'s>,
    pub(in crate::storage::queries) stream_failure: Option<error::Kind>,
}

impl<'s> Registry<'s> {
    pub(in crate::storage::queries) fn retire<const N: usize>(
        &mut self,
        row: &mut state::Row<'s, N>,
    ) {
        match mem::replace(&mut row.value, state::Branch::Empty) {
            state::Branch::Pending(probe) => {
                row.unregister(probe.request());
                probe.release(&mut self.live);
            }
            state::Branch::Waiting {
                attempt: probe,
                transaction: id,
            } => {
                probe.release(&mut self.live);
                self.ids.release(id);
            }
            state::Branch::Done { .. } | state::Branch::Empty | state::Branch::Failed(_) => {}
        }
    }
}
