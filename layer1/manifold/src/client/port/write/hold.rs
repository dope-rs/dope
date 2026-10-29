use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::{self, metadata};
use metadata::reserve;

use crate::client::{
    conn,
    port::{state, write::Permit},
};

/// A source-capacity hold tied to one exact connection generation.
#[must_use = "a source hold must be validated or rolled back"]
pub struct Hold<'a, 'token, 'e, 'd, B, ID: Identity = KeyTag<0>> {
    entry: &'a state::Entry<'d, conn::Id<'d, ID>>,
    transaction: state::Transaction<conn::Id<'d, ID>>,
    source: reserve::Reserve<'a, 'token, 'e, 'd, B>,
}

impl<'a, 'token, 'e, 'd, B, ID: Identity> Hold<'a, 'token, 'e, 'd, B, ID> {
    pub(in crate::client::port) const fn new(
        entry: &'a state::Entry<'d, conn::Id<'d, ID>>,
        txn: state::Transaction<conn::Id<'d, ID>>,
        source: reserve::Reserve<'a, 'token, 'e, 'd, B>,
    ) -> Self {
        Self {
            entry,
            transaction: txn,
            source,
        }
    }

    /// Consumes this capability and retains it only while its exact
    /// connection generation still accepts writes.
    pub fn validate(self) -> Result<Permit<'a, 'token, 'e, 'd, B, ID>, egress::NonEmpty<B>> {
        if !self.entry.can_write(self.transaction) {
            return Err(self.source.rollback());
        }
        Ok(Permit::new(self.entry, self.transaction, self.source))
    }
}
