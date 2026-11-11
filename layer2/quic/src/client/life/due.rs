use crate::runtime::brand;

use dope::core::driver::schedule::clock;

use crate::{client, conn::id};

/// Snapshot of the resident retry entry selected for one attempt.
#[must_use = "a due client attempt must be bound or deferred"]
pub(in crate::client) struct Due {
    pub(super) domain: id::Domain,
    pub(in crate::client) slot: client::SlotId,
    pub(super) attempt: client::AttemptState,
}

impl Due {
    pub(in crate::client) fn scope<'scope, 'd>(
        &self,
        brand: brand::Token<'scope>,
        started: clock::Moment<'d>,
    ) -> client::AttemptScope<'scope, 'd> {
        self.attempt.scope(brand, self.domain, self.slot, started)
    }
}
