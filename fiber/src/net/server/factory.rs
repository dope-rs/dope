use std::io;

use dope::{core::driver::storage, net::wire};

use crate::{
    net::{port, server},
    wait,
};

impl<W: wire::Wire, const ID: u8> storage::Factory for server::ListenerPortFactory<W, ID> {
    type Output<'d> = server::ListenerPort<'d, W, ID>;
    type Error = io::Error;

    fn build<'d>(
        self,
        context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        let capacity = self.layout.connections();
        let waiters = wait::Queue::try_with_capacity(context.driver(), capacity)?;
        Ok(server::ListenerPort {
            connections: port::Table::try_with_layout(self.layout, true)?,
            accepts: server::Accepts::try_with_capacity(capacity)?,
            waiters,
            wire_storage: self.wire_storage,
        })
    }
}
