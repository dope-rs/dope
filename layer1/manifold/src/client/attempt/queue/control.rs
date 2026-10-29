use std::ptr;

use dope_core::driver::{
    result,
    route::{Identity, KeyTag},
};
use dope_net::link::pool::{self, change::opening};
use o3::depot::capacity;

use crate::client::attempt::{
    self, mode,
    queue::{self, table},
};

pub struct Control<'a, 'd, T: dope_net::Transport, ID: Identity = KeyTag<0>> {
    table: &'a table::Table<'d, T, ID>,
}

impl<T: dope_net::Transport, ID: Identity> attempt::Contract for Control<'_, '_, T, ID> {
    fn identity(&self) -> Option<ptr::NonNull<()>> {
        Some(ptr::NonNull::from(self.table).cast())
    }
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> Control<'a, 'd, T, ID> {
    pub(super) fn new(table: &'a table::Table<'d, T, ID>) -> Self {
        Self { table }
    }

    pub(in crate::client) fn take(&mut self) -> Option<(attempt::Id<'d, ID>, pool::Key<'d, ID>)> {
        self.table.poll_cancel()
    }

    pub(in crate::client) fn poll(&mut self) -> attempt::Action<'d, T, ID> {
        self.table.poll()
    }

    pub(in crate::client) fn defer(
        &mut self,
        key: attempt::Id<'d, ID>,
        plan: attempt::Plan<T>,
    ) -> Result<(), (queue::Lookup<'d, ID>, attempt::Plan<T>)> {
        self.table.defer(key, plan)
    }

    pub(in crate::client) fn reject(
        &mut self,
        key: attempt::Id<'d, ID>,
    ) -> Result<(), queue::Lookup<'d, ID>> {
        self.table.reject(key)
    }

    pub(in crate::client) fn disconnect(
        &mut self,
        key: attempt::Id<'d, ID>,
    ) -> Result<(), queue::Lookup<'d, ID>> {
        self.table.free(key)
    }

    pub(in crate::client) fn bind(
        &mut self,
        key: attempt::Id<'d, ID>,
        local: pool::Key<'d, ID>,
    ) -> Result<(), queue::Lookup<'d, ID>> {
        self.table.bind(key, local)
    }

    pub(in crate::client) fn failed(
        &mut self,
        key: attempt::Id<'d, ID>,
    ) -> Result<attempt::Transition, queue::Lookup<'d, ID>> {
        self.table.fail(key)
    }
}

impl<'d, T: dope_net::Transport, ID: Identity> attempt::Control<'d, T, ID>
    for Control<'_, 'd, T, ID>
{
    type Mode = mode::Local;
    type Error = queue::Lookup<'d, ID>;

    fn resize(&mut self, cap: capacity::Count) -> Result<(), attempt::ResizeError> {
        let avail = capacity::Count::new(self.table.nodes.capacity() as u32);
        if avail < cap {
            return Err(attempt::ResizeError::new(cap, avail));
        }
        Ok(())
    }

    fn is_pending(&self) -> bool {
        self.table.heads[0].first().is_some()
    }

    fn connected(&mut self, key: attempt::Id<'d, ID>) -> Result<attempt::Connection, Self::Error> {
        Ok(match self.table.connected(key)? {
            attempt::Transition::Applied => attempt::Connection::Applied(()),
            attempt::Transition::Stale => attempt::Connection::Stale,
        })
    }

    fn rejected(
        &mut self,
        _: attempt::Id<'d, ID>,
        error: result::SubmitError,
    ) -> result::SubmitError {
        error
    }

    fn kill(
        &mut self,
        key: attempt::Id<'d, ID>,
        _: Option<opening::Exhaustion>,
    ) -> Result<(), Self::Error> {
        self.table.free(key)
    }
}
