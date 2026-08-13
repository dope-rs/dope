mod connect;
mod factory;
mod pending;

use std::{io, task};

pub use connect::Connect;
use dope::{
    core::{
        driver::{
            self,
            schedule::{self, ready::completion},
        },
        io::socket,
    },
    manifold::{
        self,
        connector::{
            self,
            app::{self, continuation},
            attempt::{self, queue},
            connection, lifecycle,
        },
        receive, timing,
    },
    net::{
        self,
        link::{
            egress::{self, data},
            event,
        },
        wire,
    },
};
pub use factory::Factory;
use o3::cell::region;

use crate::{
    context,
    net::port::{self, recv::arena},
};

pub struct Port<'d, T: net::Transport, W: wire::Wire, const ID: u8 = 0> {
    connections: port::Table<'d, W::RetainedRecv<'d>, connection::Id<'d, ID>>,
    pending: pending::Pending<'d, ID>,
    source: queue::Source<'d, T, ID>,
    wire_storage: W::ConnectionStorage<ID>,
}

impl<'d, T: net::Transport, W: wire::Wire, const ID: u8> Port<'d, T, W, ID> {
    fn from_parts(
        connections: port::Table<'d, W::RetainedRecv<'d>, connection::Id<'d, ID>>,
        pending: pending::Pending<'d, ID>,
        source: queue::Source<'d, T, ID>,
        wire_storage: W::ConnectionStorage<ID>,
    ) -> Self {
        Self {
            connections,
            pending,
            source,
            wire_storage,
        }
    }

    pub fn factory(capacity: usize) -> io::Result<Factory<T, W, ID>> {
        use io::ErrorKind;
        use o3::collections::slab::Capacity;
        let layout = arena::RecvLayout::new(capacity)?;
        let wire_storage = W::connection_storage::<ID>(layout.connections())?;
        let slab_capacity = Capacity::try_from(layout.connections())
            .map_err(|error| io::Error::new(ErrorKind::InvalidInput, error))?;
        Ok(Factory::new(layout, wire_storage, slab_capacity))
    }

    pub fn handle(&self) -> Handle<'_, 'd, T, W, ID> {
        Handle { port: self }
    }

    pub fn wire_storage(&self) -> &W::ConnectionStorage<ID> {
        &self.wire_storage
    }

    pub fn connector(
        &self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>>
    where
        W::InitConfig<'d, ID>: Default,
    {
        self.connector_with_wire(W::InitConfig::<'d, ID>::default(), driver)
    }

    pub fn connector_with_wire(
        &self,
        wire_config: W::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>> {
        use dope::manifold::connector::connection::Engine;
        let engine = Engine::with_attempt_source(
            AsyncApp { port: self },
            &self.source,
            self.connections.capacity(),
            Default::default(),
            wire_config,
            driver,
        )?;
        Ok(Connector { engine })
    }

    fn dial<'source>(
        &'source self,
        addr: T::Addr,
        config: T::StreamConfig,
    ) -> io::Result<queue::Lease<'source, 'd, T, ID>> {
        let options = T::stream_options(config)?;
        let lease = self
            .source
            .dial(attempt::StreamTarget::new(addr, options))
            .ok_or_else(|| io::Error::other("fiber::Connector: pending pool exhausted"))?;
        if !self.pending.reserve(lease.id()) {
            return Err(io::Error::other(
                "fiber::Connector: pending generation collision",
            ));
        }
        Ok(lease)
    }

    fn resolve(
        &self,
        key: attempt::Id<'d, ID>,
        wake: completion::Waker<'d>,
    ) -> task::Poll<io::Result<connection::Id<'d, ID>>> {
        use std::task::Poll;

        match self.pending.poll(key, wake) {
            Ok(Poll::Ready(pending::Outcome::Connected(token))) => Poll::Ready(Ok(token)),
            Ok(Poll::Ready(pending::Outcome::Failed(error))) => Poll::Ready(Err(error)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(pending::Stale) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }

    fn cancel(&self, key: attempt::Id<'d, ID>) {
        let _ = self.pending.cancel(key);
    }

    fn connected(
        &self,
        key: attempt::Id<'d, ID>,
        connection: connection::Id<'d, ID>,
        wake: context::RootWaker<'d>,
        region: &mut region::Token<'d>,
    ) {
        if !self.connections.activate(connection, wake, region) {
            let _ = self.pending.settle(
                key,
                pending::Outcome::Failed(io::Error::other("fiber::Connector: activation failed")),
            );
            return;
        }
        if self
            .pending
            .settle(key, pending::Outcome::Connected(connection))
            .is_err()
        {
            self.connections.channel().close(connection);
        }
    }
}

#[doc(hidden)]
pub struct AsyncApp<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> {
    port: &'scope Port<'d, T, W, ID>,
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::Application<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    type Conn = ();
    type Wire = W;
    type Send = data::Buffer<'d>;
    type Input = receive::Retained;

    fn connection(&self) -> Self::Conn {}
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::Receive<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    type Continuation = continuation::Complete;
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::RetainedReceive<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    const RETENTION: receive::Retention = arena::RecvLayout::RETENTION;

    fn retained_chunk<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        chunk: W::RetainedRecv<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        if self.port.connections.channel().push_retained(
            connection.id(),
            chunk,
            driver.region_token(),
        ) {
            app::ChunkOutcome::Overrun
        } else {
            app::ChunkOutcome::Ok
        }
    }
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::Lifecycle<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    fn connected<O>(
        &mut self,
        key: attempt::Id<'d, ID>,
        _peer: socket::Addr,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        use crate::context::RootWaker;
        self.port.connected(
            key,
            connection.id(),
            RootWaker::from(connection.wake_target()),
            driver.region_token(),
        );
    }

    fn connect_failed(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: event::ConnectFailure,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
        let _ = self
            .port
            .pending
            .settle(key, pending::Outcome::Failed(cause.into_io_error()));
    }

    fn open(
        &mut self,
        key: attempt::Id<'d, ID>,
        outcome: app::OpenOutcome<W::OpenError>,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
        if let app::OpenOutcome::Failed(error) = outcome {
            let _ = self
                .port
                .pending
                .settle(key, pending::Outcome::Failed(io::Error::other(error)));
        }
    }

    fn sent(&mut self, connection: connection::Id<'d, ID>, has_pending_egress: bool) {
        self.port
            .connections
            .channel()
            .sync_send(connection, has_pending_egress);
    }

    fn close<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        reason: lifecycle::CloseReason,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        self.port.connections.channel().closed(connection.id());
        app::CloseOutcome::Complete(reason)
    }

    fn is_drained<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        self.port
            .connections
            .channel()
            .retained_drained(connection.id())
    }
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::RequestSource<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    fn drain_requests(
        &self,
        connection: connection::Id<'d, ID>,
        _state: &mut Self::Conn,
        drain: &mut app::RequestDrain<'_, 'd, data::Buffer<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Requests {
        self.port
            .connections
            .drain_requests(connection, driver.region_token(), drain)
            .unwrap_or_default()
    }
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> app::Scheduling<'d, ID>
    for AsyncApp<'scope, 'd, T, W, ID>
{
    fn pre_park(&mut self, work: schedule::Application<'_, 'd>, region: &mut region::Token<'d>) {
        self.port.connections.maintenance().pre_park(work, region);
    }

    fn shutdown(&mut self) {
        self.port.connections.maintenance().begin_shutdown();
    }

    fn progress(&self, _region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.port.connections.maintenance().progress()
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
#[repr(transparent)]
pub struct Connector<'scope, 'd, const ID: u8, T: net::Transport, W: wire::Wire> {
    #[pin]
    #[forward('d)]
    engine: connection::Engine<
        'd,
        ID,
        AsyncApp<'scope, 'd, T, W, ID>,
        queue::Control<'scope, 'd, T, ID>,
        manifold::Bundle<T, W, timing::Balanced>,
    >,
}

pub struct Handle<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8 = 0> {
    port: &'scope Port<'d, T, W, ID>,
}

impl<T: net::Transport, W: wire::Wire, const ID: u8> Copy for Handle<'_, '_, T, W, ID> {}

impl<T: net::Transport, W: wire::Wire, const ID: u8> Clone for Handle<'_, '_, T, W, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'd, T: net::Transport, W: wire::Wire, const ID: u8> Handle<'scope, 'd, T, W, ID> {
    pub fn connect(self, addr: T::Addr, config: T::StreamConfig) -> Connect<'scope, 'd, T, W, ID> {
        Connect::new(self, addr, config)
    }
}
