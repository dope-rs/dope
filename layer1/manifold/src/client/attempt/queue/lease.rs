use std::marker;

use dope_core::driver::route::{Identity, KeyTag};
use o3::mem::{self, lease::Open};

use crate::client::attempt::{
    self,
    queue::{Held, table::Table},
};

/// Retains one exact generation until commit or cancellation.
#[must_use = "an attempt lease must be committed or cancelled"]
pub struct Lease<'source, 'd, T: dope_net::Transport, ID: Identity = KeyTag<0>> {
    held: mem::Lease<Cancel<'source, 'd, T, ID>, Open>,
}

struct Cancel<'a, 'd, T: dope_net::Transport, ID: Identity>(
    marker::PhantomData<&'a Table<'d, T, ID>>,
);

impl<'source, 'd, T: dope_net::Transport, ID: Identity> Lease<'source, 'd, T, ID> {
    pub(super) fn new(proof: Held<'source, 'd, T, ID>) -> Self {
        Self {
            held: mem::Lease::open(Cancel(marker::PhantomData), proof),
        }
    }

    pub const fn id(&self) -> attempt::Id<'d, ID> {
        *self.held.item().key()
    }

    pub fn receipt(&self) -> attempt::Completion<'source, 'd, ID> {
        attempt::Completion::queued(self.id(), *self.held.item().source())
    }

    /// Transfers a connected attempt to its connection owner.
    pub fn commit(self) {
        let (_, proof) = self.held.into_parts();
        Table::finish(proof, true);
    }

    pub fn cancel(self) {}
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> mem::Return for Cancel<'a, 'd, T, ID> {
    type Item = Held<'a, 'd, T, ID>;
    fn return_item(self, proof: Self::Item) {
        Table::finish(proof, false);
    }
}
