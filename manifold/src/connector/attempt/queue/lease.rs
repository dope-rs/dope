use std::mem;

use crate::connector::{attempt, attempt::queue::table};

/// An owner that retains one attempt generation until commit or drop.
#[must_use = "dropping an attempt lease cancels or releases its generation"]
pub struct Lease<'source, 'd, T: dope_net::Transport, const ID: u8 = 0> {
    table: &'source table::Table<'d, T, ID>,
    key: attempt::Id<'d, ID>,
}

impl<'source, 'd, T: dope_net::Transport, const ID: u8> Lease<'source, 'd, T, ID> {
    pub(super) const fn new(
        table: &'source table::Table<'d, T, ID>,
        key: attempt::Id<'d, ID>,
    ) -> Self {
        Self { table, key }
    }

    #[must_use]
    pub const fn id(&self) -> attempt::Id<'d, ID> {
        self.key
    }

    /// Transfers a successfully established attempt to the connection owner.
    pub fn commit(self) {
        let this = mem::ManuallyDrop::new(self);
        this.table.commit_lease(this.key);
    }
}

impl<T: dope_net::Transport, const ID: u8> Drop for Lease<'_, '_, T, ID> {
    fn drop(&mut self) {
        self.table.cancel(self.key);
    }
}
