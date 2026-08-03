use std::io::{self, Error};
use std::pin::Pin;
use std::task::Poll;

use dope::driver::token::Token;
use dope::manifold::env::Bundle;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::config::Config;
use dope::manifold::listener::state::{EgressCtx, State};
use dope::manifold::typed::TypedToken;
use dope::manifold::{Manifold, Outcome, listener};
use dope::runtime::dispatcher::{FinishContext, Idle};
use dope::runtime::executor::StorageFactory;
use dope::runtime::profile::Balanced;
use dope::{DriverContext, Event, hash};
use dope_net::link::egress;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::{ListenerTransport, Transport};
use o3::buffer::RetainBytes;
use o3::cell::RegionToken;
use o3::collections::CellQueue;
use pin_project::pin_project;

use super::port::Port;
use super::port::recv::arena::RecvLayout;
use crate::io::Io;
use crate::{Context, Fiber, WaitQueue, Waiter};

pub struct ListenerPort<'d, W: Wire> {
    connections: Port<'d, W::RetainedRecv<'d>>,
    accepts: CellQueue<Token>,
    waiters: Pin<Box<WaitQueue>>,
    egress: egress::storage::Storage,
    wire_storage: W::ConnectionStorage,
}

pub struct ListenerPortFactory<W: Wire> {
    layout: RecvLayout,
    egress: egress::config::Config,
    wire_storage: W::ConnectionStorage,
}

impl<'d, W: Wire> ListenerPort<'d, W> {
    fn with_layout(
        layout: RecvLayout,
        egress: egress::config::Config,
        wire_storage: W::ConnectionStorage,
    ) -> Self {
        let capacity = layout.connections();
        Self {
            connections: Port::with_layout(layout, true),
            accepts: CellQueue::with_capacity(capacity),
            waiters: Box::pin(WaitQueue::with_capacity(capacity)),
            egress: egress::storage::Storage::with_config(egress),
            wire_storage,
        }
    }

    pub fn factory(capacity: usize) -> io::Result<ListenerPortFactory<W>> {
        Self::factory_with_egress(capacity, egress::config::Config::default())
    }

    pub fn factory_with_egress(
        capacity: usize,
        egress: egress::config::Config,
    ) -> io::Result<ListenerPortFactory<W>> {
        let layout = RecvLayout::new(capacity)?;
        let wire_storage = W::connection_storage(layout.connections())?;
        Ok(ListenerPortFactory {
            layout,
            egress,
            wire_storage,
        })
    }

    fn activate(&self, token: Token, region: &mut RegionToken<'d>) -> bool {
        if !self.connections.activate_deferred(token, region) {
            return false;
        }
        if self.accepts.push_back(token).is_err() {
            return false;
        }
        self.waiters.as_ref().wake_one();
        true
    }

    pub fn handle(&self) -> ListenerHandle<'_, 'd, W> {
        ListenerHandle { port: self }
    }

    pub fn wire_storage(&self) -> &W::ConnectionStorage {
        &self.wire_storage
    }
}

impl<W: Wire> StorageFactory for ListenerPortFactory<W> {
    type Output<'d> = ListenerPort<'d, W>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ListenerPort::with_layout(self.layout, self.egress, self.wire_storage)
    }
}

struct AcceptQueue<'scope, 'd, W: Wire> {
    port: &'scope ListenerPort<'d, W>,
}

impl<'scope, 'd, W: Wire> Application<'d> for AcceptQueue<'scope, 'd, W> {
    type Conn = ();
    type Wire = W;
    type Hooks = Self;

    const RETAIN_RAW_RECV: bool = true;

    fn max_retained_recv_chunks(max_connections: usize) -> io::Result<usize> {
        RecvLayout::new(max_connections).map(RecvLayout::slots)
    }
}

impl<'scope, 'd, W: Wire> ApplicationHooks<'d, AcceptQueue<'scope, 'd, W>>
    for AcceptQueue<'scope, 'd, W>
{
    fn chunk<R: RetainBytes>(
        _app: Pin<&mut AcceptQueue<'scope, 'd, W>>,
        slot: &mut Slot<'d, W, State<()>>,
        egress: EgressCtx<'_, 'd, '_>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let _ = (slot, egress, chunk, driver);
        Outcome::Overrun
    }

    fn retained_chunk(
        app: Pin<&mut AcceptQueue<'scope, 'd, W>>,
        slot: &mut Slot<'d, W, State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        chunk: W::RetainedRecv<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        if app
            .get_mut()
            .port
            .connections
            .push_retained(slot.token(), chunk, driver.region_token())
        {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn close(
        app: Pin<&mut AcceptQueue<'scope, 'd, W>>,
        slot: &mut Slot<'d, W, State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
    ) {
        app.get_mut().port.connections.closed(slot.token());
    }

    fn accept(
        app: Pin<&mut AcceptQueue<'scope, 'd, W>>,
        slot: &mut Slot<'d, W, State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let port = app.get_mut().port;
        if port.activate(slot.token(), driver.region_token()) {
            Outcome::Ok
        } else {
            Outcome::Overrun
        }
    }
}

type Inner<'scope, 'd, const ID: u8, T, W> =
    listener::Listener<'scope, 'd, ID, AcceptQueue<'scope, 'd, W>, Bundle<T, W, Balanced>>;

#[pin_project(!Unpin)]
pub struct Listener<'scope, 'd, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    #[pin]
    inner: Inner<'scope, 'd, ID, T, W>,
    port: &'scope ListenerPort<'d, W>,
}

impl<'scope, 'd, const ID: u8, T: Transport, W: Wire> Listener<'scope, 'd, ID, T, W>
where
    T::Addr: Clone,
{
    pub fn bind(
        port: &'scope ListenerPort<'d, W>,
        driver: &mut DriverContext<'_, 'd>,
        addr: &T::Addr,
        backlog: i32,
        listener_config: T::ListenerConfig,
        stream_config: T::StreamConfig,
        hash_builder: hash::State,
    ) -> io::Result<Self>
    where
        T: ListenerTransport,
        W::InitConfig<'d>: Default,
    {
        let config = Config {
            max_connections: port.connections.capacity(),
            bind: addr.clone(),
            backlog,
            stream: stream_config,
            transport: listener_config,
            egress: Default::default(),
        };
        Self::bind_with_wire(
            port,
            driver,
            config,
            W::InitConfig::<'d>::default(),
            hash_builder,
        )
    }

    pub fn bind_with_wire(
        port: &'scope ListenerPort<'d, W>,
        driver: &mut DriverContext<'_, 'd>,
        mut config: Config<T>,
        wire_config: W::InitConfig<'d>,
        hash_builder: hash::State,
    ) -> io::Result<Self>
    where
        T: ListenerTransport,
    {
        config.max_connections = port.connections.capacity();
        let inner = listener::Listener::open_in_with_wire(
            AcceptQueue::<W> { port },
            config,
            wire_config,
            hash_builder,
            &port.egress,
            driver,
        )?;
        Ok(Self { inner, port })
    }

    fn sync_send(
        inner: Pin<&Inner<'scope, 'd, ID, T, W>>,
        port: &ListenerPort<'d, W>,
        conn: Token,
    ) {
        let pending = inner.has_pending_egress(conn);
        port.connections.sync_send(conn, pending);
    }

    fn apply_requests(mut self: Pin<&mut Self>, conn: Token, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        let requests = port.connections.requests(conn);
        if let Some(requests) = requests {
            let inner = self.as_ref().project_ref().inner.get_ref();
            if let Some(bytes) = requests.send
                && !inner.mark_send(driver.region_token(), conn, bytes)
            {
                port.connections.failed(conn);
            }
            if requests.close {
                inner.close(conn);
            }
        }
        self.as_mut().project().inner.activate(conn, driver);
        Self::sync_send(self.as_ref().project_ref().inner, port, conn);
    }
}

pub struct ListenerHandle<'scope, 'd, W: Wire> {
    port: &'scope ListenerPort<'d, W>,
}

impl<W: Wire> Copy for ListenerHandle<'_, '_, W> {}

impl<W: Wire> Clone for ListenerHandle<'_, '_, W> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'd, W: Wire> ListenerHandle<'scope, 'd, W> {
    pub fn accept(self) -> Accept<'scope, 'd, W> {
        Accept {
            host: self,
            waiter: Waiter::new(),
        }
    }
}

impl<'scope, 'd, const ID: u8, T: Transport, W: Wire> Manifold<'d>
    for Listener<'scope, 'd, ID, T, W>
where
    T::Addr: Clone,
{
    const ID: u8 = ID;

    fn dispatch(mut self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let conn = match &ev {
            Event::Send(conn, _) => Some(*conn),
            _ => None,
        };
        let port = self.as_ref().get_ref().port;
        self.as_mut().project().inner.dispatch(ev, driver);
        if let Some(conn) = conn {
            Self::sync_send(self.as_ref().project_ref().inner, port, conn);
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        while let Some(conn) = port.connections.pop_deferred_request() {
            self.as_mut().apply_requests(conn, driver);
        }
        self.project().inner.pre_park(driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.apply_requests(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        self.project_ref().inner.idle(region)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.project().inner.shutdown(driver);
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        self.project().inner.finish(context);
    }
}

#[pin_project]
pub struct Accept<'scope, 'd, W: Wire> {
    host: ListenerHandle<'scope, 'd, W>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'scope, 'd, W: Wire> Fiber<'d> for Accept<'scope, 'd, W> {
    type Output = io::Result<Io<'scope, 'd, W>>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let waiter = this.waiter;
        loop {
            let Some(id) = this.host.port.accepts.pop_front() else {
                return if this
                    .host
                    .port
                    .waiters
                    .as_ref()
                    .try_register(waiter.as_ref(), cx.as_ref())
                {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(Error::other("listener waiter capacity exhausted")))
                };
            };
            if this.host.port.connections.contains(id) {
                waiter.as_ref().unregister();
                return Poll::Ready(Ok(Io::new(&this.host.port.connections, id)));
            }
        }
    }
}
