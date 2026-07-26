use std::io;
use std::io::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use o3::buffer::RetainBytes;
use o3::collections::CellQueue;
use pin_project::pin_project;

use crate::ConnEnv;
use crate::io::{Host, Io};
use crate::{Context, Fiber, WaitQueue, Waiter};
use dope::DriverContext;
use dope::EventRef;
use dope::driver::token::Token;
use dope::hash;
use dope::io::provided::ProvidedView;
use dope::manifold::listener::application::Application;
use dope::manifold::listener::config::Config;
use dope::manifold::typed::TypedToken;
use dope::manifold::{Manifold, Outcome, listener};
use dope::runtime::dispatcher::Idle;
use dope_net::Transport;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;

use super::port::recv::arena::{RecvArena, RecvLayout};
use super::port::{Port, Requests};
use dope::Event;
use dope::manifold::listener::state::{Aux, State};
use dope::runtime::executor::StorageFactory;
use std::io::ErrorKind;

pub struct ListenerPort<'d> {
    connections: Port<'d>,
    accepts: CellQueue<Token>,
    waiters: Pin<Box<WaitQueue>>,
}

pub struct ListenerPortFactory {
    layout: RecvLayout,
}

impl<'d> ListenerPort<'d> {
    pub fn with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self::with_layout(RecvLayout::new(capacity)?))
    }

    fn with_layout(layout: RecvLayout) -> Self {
        let capacity = layout.connections();
        Self {
            connections: Port::with_layout(layout, true),
            accepts: CellQueue::with_capacity(capacity),
            waiters: Box::pin(WaitQueue::with_capacity(capacity)),
        }
    }

    pub fn factory(capacity: usize) -> io::Result<ListenerPortFactory> {
        Ok(ListenerPortFactory {
            layout: RecvLayout::new(capacity)?,
        })
    }

    fn capacity(&self) -> usize {
        self.connections.capacity()
    }

    fn activate(&self, token: Token) -> bool {
        if !self.connections.activate_deferred(token) {
            return false;
        }
        if self.accepts.push_back(token).is_err() {
            return false;
        }
        self.waiters.as_ref().wake_one();
        true
    }

    fn pop(&self) -> Option<Token> {
        self.accepts.pop_front()
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

    fn failed(&self, token: Token) {
        self.connections.failed(token);
    }

    fn requests(&self, token: Token) -> Option<Requests> {
        self.connections.requests(token)
    }

    fn pop_request(&self) -> Option<Token> {
        self.connections.pop_deferred_request()
    }

    fn sync_send(&self, token: Token, inflight: bool) {
        self.connections.sync_send(token, inflight);
    }

    pub fn handle(&self) -> ListenerHandle<'_, 'd> {
        ListenerHandle { port: self }
    }
}

impl StorageFactory for ListenerPortFactory {
    type Output<'d> = ListenerPort<'d>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ListenerPort::with_layout(self.layout)
    }
}

pub(crate) struct AcceptQueue<'scope, 'd, W: Wire> {
    port: &'scope ListenerPort<'d>,
    _wire: PhantomData<fn() -> W>,
}

impl<'scope, 'd, W: Wire> Application<'d> for AcceptQueue<'scope, 'd, W> {
    type Conn = ();
    type Wire = W;

    const RETAIN_RAW_RECV: bool = true;

    fn max_retained_recv_chunks(max_connections: usize) -> io::Result<usize> {
        RecvArena::capacity_for(max_connections)
    }

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, State<()>>,
        chunk: R,
        _aux: &mut Aux,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        if self.get_mut().port.push_recv(slot.token(), chunk) {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn retained_chunk(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, State<()>>,
        chunk: ProvidedView<'d>,
        _aux: &mut Aux,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        if self.get_mut().port.push_retained(slot.token(), chunk) {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn send(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, W, State<()>>,
        _sent: usize,
        _aux: &mut Aux,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
    }

    fn close(self: Pin<&mut Self>, slot: &mut Slot<'d, W, State<()>>, _aux: &mut Aux) {
        self.get_mut().port.closed(slot.token());
    }

    fn accept(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, W, State<()>>,
        _aux: &mut Aux,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let port = self.get_mut().port;
        if port.activate(slot.token()) {
            Outcome::Ok
        } else {
            Outcome::Overrun
        }
    }
}

type Inner<'scope, 'd, const ID: u8, T, W> =
    listener::Listener<'d, ID, AcceptQueue<'scope, 'd, W>, ConnEnv<T, W>>;

#[pin_project(!Unpin)]
pub struct Listener<'scope, 'd, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    #[pin]
    inner: Inner<'scope, 'd, ID, T, W>,
    port: &'scope ListenerPort<'d>,
}

impl<'scope, 'd, const ID: u8, T: Transport, W: Wire> Listener<'scope, 'd, ID, T, W>
where
    T::Addr: Clone,
{
    pub fn bind(
        port: &'scope ListenerPort<'d>,
        driver: &mut DriverContext<'_, 'd>,
        addr: &T::Addr,
        backlog: i32,
        listener_config: T::ListenerConfig,
        stream_config: T::StreamConfig,
        hash_builder: hash::State,
    ) -> io::Result<Self>
    where
        W::InitConfig: Default,
    {
        Self::bind_with_wire(
            port,
            driver,
            Self::config(
                port.capacity(),
                addr,
                backlog,
                listener_config,
                stream_config,
            ),
            W::InitConfig::default(),
            hash_builder,
        )
    }

    pub fn bind_with_wire(
        port: &'scope ListenerPort<'d>,
        driver: &mut DriverContext<'_, 'd>,
        config: Config<T>,
        wire_config: W::InitConfig,
        hash_builder: hash::State,
    ) -> io::Result<Self> {
        if config.max_connections != port.capacity() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "fiber: listener config capacity does not match port",
            ));
        }
        let inner = listener::Listener::open_in_with_wire(
            AcceptQueue::<W> {
                port,
                _wire: PhantomData,
            },
            config,
            wire_config,
            hash_builder,
            driver,
        )?;
        Ok(Self { inner, port })
    }

    fn config(
        capacity: usize,
        addr: &T::Addr,
        backlog: i32,
        listener_config: T::ListenerConfig,
        stream_config: T::StreamConfig,
    ) -> Config<T> {
        Config {
            max_connections: capacity,
            bind: addr.clone(),
            backlog,
            stream: stream_config,
            transport: listener_config,
            egress: Default::default(),
        }
    }

    fn sync_send(inner: Pin<&Inner<'scope, 'd, ID, T, W>>, port: &ListenerPort<'d>, conn: Token) {
        let pending = inner.has_pending_egress(conn);
        port.sync_send(conn, pending);
    }

    fn apply_requests(mut self: Pin<&mut Self>, conn: Token, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        let requests = port.requests(conn);
        let typed = unsafe { TypedToken::<Inner<'scope, 'd, ID, T, W>>::new_unchecked(conn) };
        if let Some(requests) = requests {
            let inner = unsafe { self.as_ref().map_unchecked(|s| &s.inner) }.get_ref();
            if let Some(bytes) = requests.send
                && !inner.mark_send(conn, bytes)
            {
                port.failed(conn);
            }
            if let Some(how) = requests.shutdown {
                inner.shutdown(conn, how);
            }
            if requests.close {
                inner.close(conn);
            }
        }
        Manifold::activate(
            unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) },
            typed,
            driver,
        );
        Self::sync_send(
            unsafe { self.as_ref().map_unchecked(|s| &s.inner) },
            port,
            conn,
        );
    }

    pub fn wire_runtime(self: Pin<&mut Self>) -> &mut W::RuntimeContext {
        unsafe { self.map_unchecked_mut(|listener| &mut listener.inner) }.wire_runtime()
    }
}

#[derive(Clone, Copy)]
pub struct ListenerHandle<'scope, 'd> {
    port: &'scope ListenerPort<'d>,
}

impl<'scope, 'd> ListenerHandle<'scope, 'd> {
    pub fn accept(self) -> Accept<'scope, 'd> {
        Accept {
            host: self,
            waiter: Waiter::new(),
        }
    }
}

impl<'scope, 'd> Host<'d> for ListenerHandle<'scope, 'd> {
    fn port(&self) -> &Port<'d> {
        &self.port.connections
    }
}

impl<'scope, 'd, const ID: u8, T: Transport, W: Wire> Manifold<'d>
    for Listener<'scope, 'd, ID, T, W>
where
    T::Addr: Clone,
{
    const ID: u8 = ID;

    fn dispatch(mut self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let conn = match ev.as_ref() {
            EventRef::Send(conn, _) => Some(conn),
            _ => None,
        };
        let port = self.as_ref().get_ref().port;
        Manifold::dispatch(
            unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) },
            ev,
            driver,
        );
        if let Some(conn) = conn {
            Self::sync_send(
                unsafe { self.as_ref().map_unchecked(|s| &s.inner) },
                port,
                conn,
            );
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        while let Some(conn) = port.pop_request() {
            self.as_mut().apply_requests(conn, driver);
        }
        Manifold::pre_park(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.apply_requests(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Manifold::idle(unsafe { self.map_unchecked(|s| &s.inner) })
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::shutdown(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }
}

pub struct Accept<'scope, 'd> {
    host: ListenerHandle<'scope, 'd>,
    waiter: Waiter<'d>,
}

impl<'scope, 'd> Fiber<'d> for Accept<'scope, 'd> {
    type Output = io::Result<Io<'d, ListenerHandle<'scope, 'd>>>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let waiter = unsafe { Pin::new_unchecked(&this.waiter) };
        loop {
            let Some(id) = this.host.port.pop() else {
                return if this
                    .host
                    .port
                    .waiters
                    .as_ref()
                    .try_register(waiter, cx.as_ref())
                {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(Error::other("listener waiter capacity exhausted")))
                };
            };
            if this.host.port.connections.contains(id) {
                waiter.unregister();
                return Poll::Ready(Ok(Io::new(this.host, id)));
            }
        }
    }
}
