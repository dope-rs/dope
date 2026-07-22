mod connect;
mod pending;

use std::io;
use std::marker::PhantomData;

use o3::buffer::{RetainBytes, Shared};
use o3::collections::CellQueue;

pub use connect::Connect;

use crate::ConnEnv;
use crate::Waker;
use crate::io::Host;
use dope::DriverContext;
use dope::driver::token::{SlotIndex, Token};
use dope::io::provided::ProvidedView;
use dope::manifold::connector::source::{DialKey, Explicit, ExplicitDialer};
use dope::manifold::connector::{ChunkOutcome, ConnApp, Core, Requests, state};
use dope_net::Transport;
use dope_net::link::egress::config::Config;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use std::io::Error;

use super::port::Port;
use super::port::recv::arena::{RecvArena, RecvLayout};
use pending::{Pending, Resolve};

enum Resolved {
    Ready(Token),
    Failed,
}

pub struct ConnectorPort<'d, T: Transport>
where
    T::Addr: Clone,
{
    connections: Port<'d>,
    pending: Pending<'d, DialKey, Resolved>,
    cancels: CellQueue<(DialKey, SlotIndex)>,
    source: Explicit<T>,
    egress: Config,
}

pub struct ConnectorPortFactory<T> {
    layout: RecvLayout,
    egress: Config,
    transport: PhantomData<fn() -> T>,
}

impl<'d, T: Transport> ConnectorPort<'d, T>
where
    T::Addr: Clone,
{
    pub fn with_capacity(capacity: usize) -> io::Result<Self> {
        Self::with_egress(capacity, Config::default())
    }

    pub fn with_egress(capacity: usize, egress: Config) -> io::Result<Self> {
        Ok(Self::with_layout(RecvLayout::new(capacity)?, egress))
    }

    fn with_layout(layout: RecvLayout, egress: Config) -> Self {
        let capacity = layout.connections();
        Self {
            connections: Port::with_layout(layout, false),
            pending: Pending::with_capacity(capacity),
            cancels: CellQueue::with_capacity(capacity),
            source: Explicit::with_capacity(capacity),
            egress,
        }
    }

    pub fn factory(capacity: usize) -> io::Result<ConnectorPortFactory<T>> {
        Ok(ConnectorPortFactory {
            layout: RecvLayout::new(capacity)?,
            egress: Config::default(),
            transport: PhantomData,
        })
    }

    pub fn factory_with_egress(
        capacity: usize,
        egress: Config,
    ) -> io::Result<ConnectorPortFactory<T>> {
        Ok(ConnectorPortFactory {
            layout: RecvLayout::new(capacity)?,
            egress,
            transport: PhantomData,
        })
    }

    pub fn handle(&self) -> ConnectorHandle<'_, 'd, T> {
        ConnectorHandle { port: self }
    }

    pub fn connector<const ID: u8, W: Wire>(
        &self,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Connector<'_, 'd, ID, T, W>> {
        Connector::with_app_config(
            AsyncApp {
                port: self,
                _wire: PhantomData,
            },
            self.source.dialer(),
            self.connections.capacity(),
            self.egress,
            driver,
        )
    }

    fn dial(&self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        let key = self.source.dial_shared(addr, config)?;
        self.pending.reserve(key);
        Some(key)
    }

    fn resolve(&self, key: DialKey, waker: Waker<'d>) -> ConnectOutcome {
        match self.pending.poll(key, waker) {
            Resolve::Ready(Resolved::Ready(token)) => ConnectOutcome::Conn(token),
            Resolve::Ready(Resolved::Failed) => {
                ConnectOutcome::Failed(Error::other("fiber::Connector: connect failed"))
            }
            Resolve::Pending => ConnectOutcome::Pending,
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
            self.pending.settle(key, Resolved::Failed);
            return;
        }
        self.pending.settle(key, Resolved::Ready(token));
    }

    fn connect_failed(&self, key: DialKey) {
        self.pending.settle(key, Resolved::Failed);
    }

    fn push_recv<R: RetainBytes>(&self, token: Token, chunk: R) -> bool {
        self.connections.push_recv(token, chunk)
    }

    fn push_retained(&self, token: Token, chunk: ProvidedView<'d>) -> bool {
        self.connections.push_retained(token, chunk)
    }

    fn closed(&self, token: Token) {
        self.connections.closed(token);
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Shared) -> Result<(), Shared>,
    ) -> Requests {
        self.connections
            .drain_requests(token, push)
            .unwrap_or_default()
    }

    fn take_cancel(&self) -> Option<(DialKey, SlotIndex)> {
        self.cancels.pop_front()
    }

    fn sync_send(&self, token: Token, inflight: bool) {
        self.connections.sync_send(token, inflight);
    }
}

impl<T> dope::runtime::StorageFactory for ConnectorPortFactory<T>
where
    T: Transport + 'static,
    T::Addr: Clone,
{
    type Output<'d> = ConnectorPort<'d, T>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ConnectorPort::with_layout(self.layout, self.egress)
    }
}

enum ConnectOutcome {
    Conn(Token),
    Failed(io::Error),
    Pending,
}

pub struct AsyncApp<'scope, 'd, T: Transport, W: Wire>
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
        RecvArena::capacity_for(max_connections)
    }

    fn chunk<R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, state::State<Self::Conn>>,
        chunk: R,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if self.port.push_recv(slot.token(), chunk) {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn retained_chunk(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, state::State<Self::Conn, Self::Send>>,
        chunk: ProvidedView<'d>,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if self.port.push_retained(slot.token(), chunk) {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn connected(
        &mut self,
        key: DialKey,
        slot: &mut Slot<'d, Self::Wire, state::State<Self::Conn>>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port.connected(
            key,
            slot.token(),
            Waker::from_ready(slot.driver(), slot.ready_key()),
        );
    }

    fn connect_failed(&mut self, key: DialKey, _driver: &mut DriverContext<'_, '_>) {
        self.port.connect_failed(key);
    }

    fn send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, state::State<Self::Conn>>,
        _sent: usize,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port
            .sync_send(slot.token(), slot.state.egress_len() != 0);
    }

    fn close(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, state::State<Self::Conn>>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.port.closed(slot.token());
    }

    fn is_drained(
        &self,
        slot: &Slot<'d, Self::Wire, state::State<Self::Conn>>,
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
        self.port.drain_requests(token, push)
    }

    fn take_cancel(&self) -> Option<(DialKey, SlotIndex)> {
        self.port.take_cancel()
    }
}

pub type Connector<'scope, 'd, const ID: u8, T, W> =
    Core<'d, ID, AsyncApp<'scope, 'd, T, W>, ExplicitDialer<'scope, T>, ConnEnv<T, W>>;

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

impl<'scope, 'd, T: Transport> Host<'d> for ConnectorHandle<'scope, 'd, T>
where
    T::Addr: Clone,
{
    fn port(&self) -> &Port<'d> {
        &self.port.connections
    }
}
