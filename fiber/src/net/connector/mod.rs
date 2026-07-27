mod pending;
pub mod raw;

use std::io::{self, Error};
use std::marker::PhantomData;
use std::task::Poll;

use o3::buffer::{RetainBytes, Shared};
use o3::collections::CellQueue;

use crate::Waker;
use dope::DriverContext;
use dope::driver::token::{SlotIndex, Token};
use dope::io::provided::ProvidedView;
use dope::manifold::connector::app::{ChunkOutcome, ConnApp, Requests};
use dope::manifold::connector::core::Core;
use dope::manifold::connector::source::DialKey;
use dope::manifold::connector::source::explicit::{Explicit, ExplicitDialer};
use dope::manifold::env::Bundle;
use dope::runtime::profile::Balanced;
use dope_net::Transport;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;

use super::port::Port;
use super::port::recv::arena::RecvLayout;
use dope::manifold::connector::state::State;
use dope::runtime::executor::StorageFactory;
use pending::{Outcome, Pending};
use raw::Connect;

pub struct ConnectorPort<'d, T: Transport>
where
    T::Addr: Clone,
{
    connections: Port<'d>,
    pending: Pending<'d>,
    cancels: CellQueue<(DialKey, SlotIndex)>,
    source: Explicit<T>,
}

pub struct ConnectorPortFactory<T> {
    layout: RecvLayout,
    transport: PhantomData<fn() -> T>,
}

impl<'d, T: Transport> ConnectorPort<'d, T>
where
    T::Addr: Clone,
{
    fn with_layout(layout: RecvLayout) -> Self {
        let capacity = layout.connections();
        Self {
            connections: Port::with_layout(layout, false),
            pending: Pending::with_capacity(capacity),
            cancels: CellQueue::with_capacity(capacity),
            source: Explicit::with_capacity(capacity),
        }
    }

    pub fn factory(capacity: usize) -> io::Result<ConnectorPortFactory<T>> {
        Ok(ConnectorPortFactory {
            layout: RecvLayout::new(capacity)?,
            transport: PhantomData,
        })
    }

    pub fn handle(&self) -> ConnectorHandle<'_, 'd, T> {
        ConnectorHandle { port: self }
    }

    pub fn connector<const ID: u8, W: Wire>(
        &self,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>>
    where
        W::InitConfig: Default,
    {
        self.connector_with_wire(W::InitConfig::default(), driver)
    }

    pub fn connector_with_wire<const ID: u8, W: Wire>(
        &self,
        wire_config: W::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>> {
        let core = Core::with_app_configs(
            AsyncApp {
                port: self,
                _wire: PhantomData,
            },
            self.source.dialer(),
            self.connections.capacity(),
            Default::default(),
            wire_config,
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

    fn resolve(&self, key: DialKey, waker: Waker<'d>) -> Poll<io::Result<Token>> {
        match self.pending.poll(key, waker) {
            Poll::Ready(Outcome::Connected(token)) => Poll::Ready(Ok(token)),
            Poll::Ready(Outcome::Failed) => {
                Poll::Ready(Err(Error::other("fiber::Connector: connect failed")))
            }
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

    fn connected(&self, key: DialKey, token: Token, wake: Waker<'d>) {
        if !self.connections.activate(token, wake) {
            self.pending.settle(key, Outcome::Failed);
            return;
        }
        self.pending.settle(key, Outcome::Connected(token));
    }
}

impl<T> StorageFactory for ConnectorPortFactory<T>
where
    T: Transport + 'static,
    T::Addr: Clone,
{
    type Output<'d> = ConnectorPort<'d, T>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ConnectorPort::with_layout(self.layout)
    }
}

struct AsyncApp<'scope, 'd, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    port: &'scope ConnectorPort<'d, T>,
    _wire: PhantomData<fn() -> W>,
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

    fn chunk<R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: R,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if self.port.connections.push_recv(slot.token(), chunk) {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn retained_chunk(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        chunk: ProvidedView<'d>,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if self.port.connections.push_retained(slot.token(), chunk) {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn connected(
        &mut self,
        key: DialKey,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port.connected(
            key,
            slot.token(),
            Waker::from_ready(slot.driver(), slot.ready_key()),
        );
    }

    fn connect_failed(&mut self, key: DialKey, _driver: &mut DriverContext<'_, '_>) {
        self.port.pending.settle(key, Outcome::Failed);
    }

    fn send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _sent: usize,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port
            .connections
            .sync_send(slot.token(), slot.state.egress_len() != 0);
    }

    fn close(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
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
        push: impl FnMut(Shared) -> Result<(), Shared>,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> Requests {
        self.port
            .connections
            .drain_requests(token, push)
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
    core:
        Core<'d, ID, AsyncApp<'scope, 'd, T, W>, ExplicitDialer<'scope, T>, Bundle<T, W, Balanced>>,
}

pub struct ConnectorHandle<'scope, 'd, T: Transport>
where
    T::Addr: Clone,
{
    port: &'scope ConnectorPort<'d, T>,
}

impl<T: Transport> Copy for ConnectorHandle<'_, '_, T> where T::Addr: Clone {}

impl<T: Transport> Clone for ConnectorHandle<'_, '_, T>
where
    T::Addr: Clone,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'd, T: Transport> ConnectorHandle<'scope, 'd, T>
where
    T::Addr: Clone,
{
    pub fn connect(self, addr: T::Addr, config: T::StreamConfig) -> Connect<'scope, 'd, T> {
        Connect::new(self, addr, config)
    }
}
