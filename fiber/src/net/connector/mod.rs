mod pending;
pub mod raw;

use std::io::{self, Error};
use std::marker::PhantomData;
use std::task::Poll;

use dope::DriverContext;
use dope::driver::ready::CompletionWaker;
use dope::driver::token::{SlotIndex, Token};
use dope::manifold::connector::app::{ChunkOutcome, ConnApp, Requests};
use dope::manifold::connector::core::Core;
use dope::manifold::connector::source::DialKey;
use dope::manifold::connector::source::explicit::{Explicit, ExplicitDialer};
use dope::manifold::connector::state::{IOV_CAP, State};
use dope::manifold::env::Bundle;
use dope::runtime::executor::StorageFactory;
use dope::runtime::profile::Balanced;
use dope_net::Transport;
use dope_net::link::egress;
use dope_net::link::egress::queue::Queue;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::{RetainBytes, Shared};
use o3::cell::RegionToken;
use o3::collections::CellQueue;
use pending::{Outcome, Pending};
use raw::Connect;

use super::port::Port;
use super::port::recv::arena::RecvLayout;
use crate::raw::task::RootWaker;

pub struct ConnectorPort<'d, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    connections: Port<'d, W::RetainedRecv<'d>>,
    pending: Pending<'d>,
    cancels: CellQueue<(DialKey, SlotIndex)>,
    source: Explicit<T>,
    egress: egress::storage::Storage,
    wire_storage: W::ConnectionStorage,
}

pub struct ConnectorPortFactory<T, W: Wire> {
    layout: RecvLayout,
    wire_storage: W::ConnectionStorage,
    transport: PhantomData<fn() -> T>,
}

impl<'d, T: Transport, W: Wire> ConnectorPort<'d, T, W>
where
    T::Addr: Clone,
{
    fn with_layout(layout: RecvLayout, wire_storage: W::ConnectionStorage) -> Self {
        let capacity = layout.connections();
        Self {
            connections: Port::with_layout(layout, false),
            pending: Pending::with_capacity(capacity),
            cancels: CellQueue::with_capacity(capacity),
            source: Explicit::with_capacity(capacity),
            egress: egress::storage::Storage::default(),
            wire_storage,
        }
    }

    pub fn factory(capacity: usize) -> io::Result<ConnectorPortFactory<T, W>> {
        let layout = RecvLayout::new(capacity)?;
        let wire_storage = W::connection_storage(layout.connections())?;
        Ok(ConnectorPortFactory {
            layout,
            wire_storage,
            transport: PhantomData,
        })
    }

    pub fn handle(&self) -> ConnectorHandle<'_, 'd, T, W> {
        ConnectorHandle { port: self }
    }

    pub fn wire_storage(&self) -> &W::ConnectionStorage {
        &self.wire_storage
    }

    pub fn connector<const ID: u8>(
        &self,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>>
    where
        W::InitConfig<'d>: Default,
    {
        self.connector_with_wire(W::InitConfig::<'d>::default(), driver)
    }

    pub fn connector_with_wire<const ID: u8>(
        &self,
        wire_config: W::InitConfig<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>> {
        let core = Core::with_app_configs(
            AsyncApp { port: self },
            self.source.dialer(),
            self.connections.capacity(),
            Default::default(),
            wire_config,
            &self.egress,
            driver,
        )?;
        Ok(Connector { core })
    }

    fn dial(&self, addr: T::Addr, config: T::StreamConfig) -> io::Result<DialKey> {
        T::validate_stream_config(config)?;
        let key = self
            .source
            .dial_shared(addr, config)
            .ok_or_else(|| Error::other("fiber::Connector: pending pool exhausted"))?;
        self.pending.reserve(key);
        Ok(key)
    }

    fn resolve(&self, key: DialKey, wake: CompletionWaker<'d>) -> Poll<io::Result<Token>> {
        match self.pending.poll(key, wake) {
            Poll::Ready(Outcome::Connected(token)) => Poll::Ready(Ok(token)),
            Poll::Ready(Outcome::Failed(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn cancel(&self, key: DialKey) {
        self.pending.cancel(key);
        let Some(local) = self.source.kill_shared(key) else {
            return;
        };
        assert!(self.cancels.push_back((key, local)).is_ok());
    }

    fn connected(
        &self,
        key: DialKey,
        token: Token,
        wake: RootWaker<'d>,
        region: &mut RegionToken<'d>,
    ) {
        if !self.connections.activate(token, wake, region) {
            self.pending.settle(
                key,
                Outcome::Failed(Error::other("fiber::Connector: activation failed")),
            );
            return;
        }
        self.pending.settle(key, Outcome::Connected(token));
    }
}

impl<T, W> StorageFactory for ConnectorPortFactory<T, W>
where
    T: Transport + 'static,
    T::Addr: Clone,
    W: Wire,
{
    type Output<'d> = ConnectorPort<'d, T, W>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ConnectorPort::with_layout(self.layout, self.wire_storage)
    }
}

struct AsyncApp<'scope, 'd, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    port: &'scope ConnectorPort<'d, T, W>,
}

impl<'scope, 'd, T: Transport, W: Wire> ConnApp<'d> for AsyncApp<'scope, 'd, T, W>
where
    T::Addr: Clone,
{
    type Conn = ();
    type Wire = W;
    type Send = Shared;

    const RETAIN_RAW_RECV: bool = true;

    fn max_retained_recv_chunks(max_connections: usize) -> io::Result<usize> {
        RecvLayout::new(max_connections).map(RecvLayout::slots)
    }

    fn chunk<'pool, R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        egress: Queue<'_, 'd, 'pool, IOV_CAP, Self::Send>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        let _ = (slot, egress, chunk, driver);
        ChunkOutcome::Overrun
    }

    fn retained_chunk<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        _egress: Queue<'_, 'd, 'pool, IOV_CAP, Self::Send>,
        chunk: W::RetainedRecv<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if self
            .port
            .connections
            .push_retained(slot.token(), chunk, driver.region_token())
        {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn connected<'pool>(
        &mut self,
        key: DialKey,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _egress: Queue<'_, 'd, 'pool, IOV_CAP, Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port.connected(
            key,
            slot.token(),
            RootWaker::from_ready(slot.driver(), slot.ready_key()),
            driver.region_token(),
        );
    }

    fn connect_failed(&mut self, key: DialKey, _driver: &mut DriverContext<'_, '_>) {
        self.port.pending.settle(
            key,
            Outcome::Failed(Error::other("fiber::Connector: connect failed")),
        );
    }

    fn open_failed(
        &mut self,
        key: DialKey,
        error: dope_net::link::raw::pool::outbound::OpenFailure<W::OpenError>,
        _driver: &mut DriverContext<'_, '_>,
    ) {
        self.port
            .pending
            .settle(key, Outcome::Failed(Error::other(error)));
    }

    fn send<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        egress: Queue<'_, 'd, 'pool, IOV_CAP, Self::Send>,
        _sent: usize,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port
            .connections
            .sync_send(slot.token(), egress.total_bytes() != 0);
    }

    fn close<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _egress: Queue<'_, 'd, 'pool, IOV_CAP, Self::Send>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port.connections.closed(slot.token());
    }

    fn is_drained(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn>>,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.port.connections.readable_drained(slot.token())
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(&mut RegionToken<'d>, Shared) -> Result<(), Shared>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Requests {
        self.port
            .connections
            .drain_requests(token, driver.region_token(), push)
            .unwrap_or_default()
    }

    fn take_cancel(&self) -> Option<(DialKey, SlotIndex)> {
        self.port.cancels.pop_front()
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
#[repr(transparent)]
pub struct Connector<'scope, 'd, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    #[pin]
    #[forward('d)]
    core: Core<
        'scope,
        'd,
        ID,
        AsyncApp<'scope, 'd, T, W>,
        ExplicitDialer<'scope, T>,
        Bundle<T, W, Balanced>,
    >,
}

pub struct ConnectorHandle<'scope, 'd, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    port: &'scope ConnectorPort<'d, T, W>,
}

impl<T: Transport, W: Wire> Copy for ConnectorHandle<'_, '_, T, W> where T::Addr: Clone {}

impl<T: Transport, W: Wire> Clone for ConnectorHandle<'_, '_, T, W>
where
    T::Addr: Clone,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'd, T: Transport, W: Wire> ConnectorHandle<'scope, 'd, T, W>
where
    T::Addr: Clone,
{
    pub fn connect(self, addr: T::Addr, config: T::StreamConfig) -> Connect<'scope, 'd, T, W> {
        Connect::new(self, addr, config)
    }
}
