use dope_net::link::egress::{data, metadata::arena};
use o3::cell::region;

use crate::connector::{
    connection,
    port::{self, state},
};

type ConnectionId<'d, const ID: u8> = connection::Id<'d, ID>;

pub struct Sender<'a, 'd, B, const ID: u8 = 0> {
    slot: arena::Slot<'a, 'd, B, state::Entry<'d, ConnectionId<'d, ID>>>,
    transaction: state::Transaction<ConnectionId<'d, ID>>,
}

impl<'a, 'd, B: data::Payload<'d>, const ID: u8> Sender<'a, 'd, B, ID> {
    pub(super) const fn new(
        slot: arena::Slot<'a, 'd, B, state::Entry<'d, ConnectionId<'d, ID>>>,
        transaction: state::Transaction<ConnectionId<'d, ID>>,
    ) -> Self {
        Self { slot, transaction }
    }

    pub fn try_enqueue(&self, token: &mut region::Token<'d>, value: B) -> Result<(), B> {
        let entry = self.slot.state();
        if !entry.is_active(self.transaction) {
            return Err(value);
        }
        if value.as_ref().is_empty() {
            drop(value);
            return Ok(());
        }
        self.slot.queue().try_push_back(token, value)?;
        entry.mark_ready(self.transaction);
        Ok(())
    }

    pub fn batch<'token>(
        &self,
        token: &'token mut region::Token<'d>,
    ) -> Option<port::Batch<'a, 'token, 'd, B, ID>> {
        self.slot.state().is_active(self.transaction).then(|| {
            port::Batch::new(
                self.slot,
                self.transaction,
                self.slot.queue().prepare(token),
            )
        })
    }
}
