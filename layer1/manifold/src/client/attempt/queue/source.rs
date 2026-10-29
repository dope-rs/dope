use std::{cell, mem};

use dope_core::driver::{
    route::{Identity, KeyTag, lease},
    storage,
};
use o3::depot::{arena::slab, capacity};

use crate::{
    app::creation,
    client::attempt::{
        Completion,
        queue::{self, table},
    },
};

/// A driver-region and route branded attempt queue.
pub struct Source<'d, T: dope_net::Transport, ID: Identity = KeyTag<0>> {
    table: table::Table<'d, T, ID>,
    route: cell::Cell<Option<lease::Claim<'d, ID>>>,
}

impl<'d, T: dope_net::Transport, ID: Identity> Source<'d, T, ID> {
    /// Constructs the sole attempt domain for a route reserved in this driver.
    pub fn try_new(
        max: capacity::Count,
        store: &mut storage::Context<'_, 'd>,
    ) -> Result<Self, creation::Error> {
        let table = table::Table::try_new(max).map_err(creation::Error::Queue)?;
        let route = store
            .reborrow()
            .try_reserve::<ID>()
            .map_err(creation::Error::Route)?;
        Ok(Self {
            table,
            route: cell::Cell::new(Some(route)),
        })
    }

    pub(crate) fn take_route(&self) -> Option<lease::Claim<'d, ID>> {
        self.route.take()
    }

    pub(in crate::client) fn control(&self) -> queue::Control<'_, 'd, T, ID> {
        queue::Control::new(&self.table)
    }

    pub(super) fn is_source(&self, receipt: &Completion<'_, 'd, ID>) -> bool {
        receipt.belongs_to(&self.table)
    }

    /// Atomically reserves one exact attempt generation before target work.
    pub fn try_reserve<'source>(
        &'source self,
    ) -> Result<queue::Reserve<'source, 'd, T, ID>, slab::Vacancy> {
        queue::Reserve::try_new(&self.table)
    }

    /// Returns whether the authoritative attempt slab currently has a vacancy.
    pub fn has_room(&self) -> bool {
        !self.table.nodes.is_full()
    }

    /// Returns whether every physical slot permanently exhausted its identities.
    pub fn is_exhausted(&self) -> bool {
        self.table.nodes.capacity() != 0 && self.table.nodes.remaining() == capacity::Count::EMPTY
    }
}

const _: () = assert!(
    mem::size_of::<Result<queue::Reserve<'static, 'static, dope_net::tcp::Tcp>, slab::Vacancy>>()
        == mem::size_of::<queue::Reserve<'static, 'static, dope_net::tcp::Tcp>>()
);
