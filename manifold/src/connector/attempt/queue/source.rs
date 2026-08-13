use std::{cell, io};

use dope_core::driver::{lifecycle::routing, storage};
use o3::collections::slab;

use crate::connector::{
    attempt,
    attempt::queue::{self, table},
};

struct RouteSlot<'d, const ID: u8> {
    reserved: cell::Cell<Option<routing::Reserved<'d, ID>>>,
}

impl<'d, const ID: u8> RouteSlot<'d, ID> {
    fn new(reserved: routing::Reserved<'d, ID>) -> Self {
        Self {
            reserved: cell::Cell::new(Some(reserved)),
        }
    }

    fn take(&self) -> Option<routing::Route<'d, ID>> {
        self.reserved.take().map(routing::Reserved::bind)
    }
}

/// A driver-region and route branded attempt queue.
pub struct Source<'d, T: dope_net::Transport, const ID: u8 = 0> {
    table: table::Table<'d, T, ID>,
    route: RouteSlot<'d, ID>,
}

impl<'d, T: dope_net::Transport, const ID: u8> Source<'d, T, ID> {
    /// Constructs the sole attempt domain for a route reserved in this driver.
    #[doc(hidden)]
    pub fn with_capacity(
        capacity: slab::Capacity,
        context: &mut storage::Context<'_, 'd>,
    ) -> io::Result<Self> {
        let table = table::Table::try_with_capacity(capacity)?;
        let route = context.reserve_route::<ID>()?;
        Ok(Self {
            table,
            route: RouteSlot::new(route),
        })
    }

    pub(crate) fn take_route(&self) -> Option<routing::Route<'d, ID>> {
        self.route.take()
    }

    pub(in crate::connector) fn control(&self) -> queue::Control<'_, 'd, T, ID> {
        queue::Control::new(&self.table)
    }

    /// Creates a non-copying owner for an attempt generation.
    #[doc(hidden)]
    pub fn dial<'source>(
        &'source self,
        target: attempt::StreamTarget<T::Addr>,
    ) -> Option<queue::Lease<'source, 'd, T, ID>> {
        let key = self.table.dial(target)?;
        Some(queue::Lease::new(&self.table, key))
    }
}
