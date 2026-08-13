use std::{io, marker};

use dope::{
    core::driver::storage,
    manifold::connector::attempt::queue,
    net::{self, wire},
};
use o3::collections::slab;

use crate::net::{
    connector::{self, pending},
    port::{self, recv::arena},
};

pub struct Factory<T: net::Transport, W: wire::Wire, const ID: u8 = 0> {
    layout: arena::RecvLayout,
    wire_storage: W::ConnectionStorage<ID>,
    capacity: slab::Capacity,
    _transport: marker::PhantomData<fn() -> T>,
}

impl<T: net::Transport, W: wire::Wire, const ID: u8> Factory<T, W, ID> {
    pub(super) fn new(
        layout: arena::RecvLayout,
        wire_storage: W::ConnectionStorage<ID>,
        capacity: slab::Capacity,
    ) -> Self {
        Self {
            layout,
            wire_storage,
            capacity,
            _transport: marker::PhantomData,
        }
    }
}

impl<T, W, const ID: u8> storage::Factory for Factory<T, W, ID>
where
    T: net::Transport + 'static,
    W: wire::Wire,
{
    type Output<'d> = connector::Port<'d, T, W, ID>;
    type Error = io::Error;

    fn build<'d>(
        self,
        context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        let capacity = self.layout.connections();
        let connections = port::Table::try_with_layout(self.layout, false)?;
        let pending = pending::Pending::try_with_capacity(capacity)?;
        let source = queue::Source::with_capacity(self.capacity, context)?;
        Ok(connector::Port::from_parts(
            connections,
            pending,
            source,
            self.wire_storage,
        ))
    }
}
