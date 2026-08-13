use std::time;

use dope_core::io::socket::option;
use dope_net::link::pool;

use crate::connector::{
    attempt::{self, queue::table},
    lifecycle,
};

pub struct Control<'a, 'd, T: dope_net::Transport, const ID: u8 = 0> {
    table: &'a table::Table<'d, T, ID>,
}

impl<T: dope_net::Transport, const ID: u8> attempt::Contract for Control<'_, '_, T, ID> {}

impl<'a, 'd, T: dope_net::Transport, const ID: u8> Control<'a, 'd, T, ID> {
    pub(super) fn new(table: &'a table::Table<'d, T, ID>) -> Self {
        Self { table }
    }
}

impl<'d, T: dope_net::Transport, const ID: u8> attempt::Control<'d, T, ID>
    for Control<'_, 'd, T, ID>
{
    fn resize(&mut self, max_connections: usize) -> Result<(), attempt::ResizeError> {
        let available = self.table.capacity();
        if available < max_connections {
            return Err(attempt::ResizeError::new(max_connections, available));
        }
        Ok(())
    }

    fn poll_connect(&mut self, _now: time::Instant) -> attempt::Action<'d, T, ID> {
        self.table.poll_connect()
    }

    fn has_pending(&self) -> bool {
        self.table.has_pending()
    }

    fn connect_succeeded(
        &mut self,
        key: attempt::Id<'d, ID>,
        _now: time::Instant,
    ) -> attempt::Transition {
        self.table.connect_succeeded(key)
    }

    fn connect_failed(&mut self, key: attempt::Id<'d, ID>, _now: time::Instant) {
        self.table.fail_connect(key);
    }

    fn connect_options(&mut self, key: attempt::Id<'d, ID>) -> Option<option::StreamOptions> {
        self.table.connect_options(key)
    }

    fn connect_deferred(
        &mut self,
        key: attempt::Id<'d, ID>,
        plan: attempt::Plan<T>,
        _now: time::Instant,
    ) {
        self.table.connect_deferred(key, plan);
    }

    fn disconnect(
        &mut self,
        key: attempt::Id<'d, ID>,
        _reason: lifecycle::CloseReason,
        _now: time::Instant,
    ) {
        self.table.free(key);
    }

    fn kill(&mut self, key: attempt::Id<'d, ID>) {
        self.table.free(key);
    }

    fn bind(
        &mut self,
        key: attempt::Id<'d, ID>,
        binding: pool::Key<'d, ID>,
        options: Option<option::StreamOptions>,
    ) {
        self.table.set_binding(key, binding, options);
    }

    fn take_cancel(&mut self) -> Option<(attempt::Id<'d, ID>, pool::Key<'d, ID>)> {
        self.table.take_cancel()
    }
}
