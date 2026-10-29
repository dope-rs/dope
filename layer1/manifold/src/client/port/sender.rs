use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::{
    data,
    metadata::{arena, batch},
};
use o3::base::cell::region;

use crate::client::{
    conn,
    port::{batch::Error, state},
};

pub struct Sender<'a, 'e, 'd, B, ID: Identity = KeyTag<0>> {
    slot: arena::Slot<'a, 'e, 'd, B, state::Entry<'d, conn::Id<'d, ID>>>,
    transaction: state::Transaction<conn::Id<'d, ID>>,
}

impl<'a, 'e, 'd, B: data::raw::Payload<'d>, ID: Identity> Sender<'a, 'e, 'd, B, ID> {
    pub(super) const fn new(
        slot: arena::Slot<'a, 'e, 'd, B, state::Entry<'d, conn::Id<'d, ID>>>,
        txn: state::Transaction<conn::Id<'d, ID>>,
    ) -> Self {
        Self {
            slot,
            transaction: txn,
        }
    }

    /// Read-only admission preflight for an exact atomic batch. This does not
    /// debit credits; callers must perform the actual transaction without
    /// yielding.
    pub fn can_admit(&self, plan: batch::Plan) -> bool {
        self.slot.state().can_write(self.transaction) && self.slot.queue().can_acquire(plan)
    }

    pub fn try_batch<'token, R, E>(
        &self,
        token: &'token mut region::Token<region::Identity<'d>>,
        plan: batch::Plan,
        build: impl for<'batch> FnOnce(batch::Sink<'batch, 'a, 'token, 'e, 'd, B>) -> Result<R, E>,
    ) -> Result<R, Error<E>> {
        let entry = self.slot.state();
        if !entry.can_write(self.transaction) {
            return Err(Error::Inactive);
        }
        let output =
            self.slot
                .queue()
                .try_batch(token, plan, build)
                .map_err(|error| match error {
                    batch::Error::Admission(cause) => Error::Admission(cause),
                    batch::Error::Build(error) => Error::Build(error),
                    batch::Error::Mismatch { remaining } => Error::Mismatch { remaining },
                })?;
        entry.mark_ready(self.transaction);
        Ok(output)
    }
}
