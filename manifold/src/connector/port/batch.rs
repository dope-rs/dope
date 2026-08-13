use dope_net::link::egress::{
    data,
    metadata::{self, arena},
};

use crate::connector::{connection, port::state};

type ConnectionId<'d, const ID: u8> = connection::Id<'d, ID>;

/// An off-queue request batch bound to one exact connection generation.
#[must_use]
pub struct Batch<'entry, 'token, 'd, B, const ID: u8 = 0> {
    slot: arena::Slot<'entry, 'd, B, state::Entry<'d, ConnectionId<'d, ID>>>,
    transaction: state::Transaction<ConnectionId<'d, ID>>,
    prepared: metadata::Prepared<'entry, 'token, 'd, B>,
}

impl<'entry, 'token, 'd, B, const ID: u8> Batch<'entry, 'token, 'd, B, ID> {
    pub(super) const fn new(
        slot: arena::Slot<'entry, 'd, B, state::Entry<'d, ConnectionId<'d, ID>>>,
        transaction: state::Transaction<ConnectionId<'d, ID>>,
        prepared: metadata::Prepared<'entry, 'token, 'd, B>,
    ) -> Self {
        Self {
            slot,
            transaction,
            prepared,
        }
    }
}

impl<'d, B: data::Payload<'d>, const ID: u8> Batch<'_, '_, 'd, B, ID> {
    pub fn try_push(&mut self, value: B) -> Result<(), B> {
        self.prepared.try_push(value)
    }

    /// Publishes every prepared request with one credit acquisition, one tail
    /// splice, and one wake. Failure leaves the queue unchanged.
    pub fn commit(self) -> bool {
        let Self {
            slot,
            transaction,
            prepared,
        } = self;
        let entry = slot.state();
        if !entry.is_active(transaction) {
            return false;
        }
        let empty = prepared.is_empty();
        if !prepared.commit() {
            return false;
        }
        if !empty {
            entry.mark_ready(transaction);
        }
        true
    }
}
