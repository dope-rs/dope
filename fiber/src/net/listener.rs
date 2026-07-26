use std::io::{self, Error};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use o3::buffer::RetainBytes;
use o3::collections::CellQueue;
use pin_project::pin_project;

use crate::io::Io;
use crate::{Context, Fiber, WaitQueue, Waiter};
use dope::DriverContext;
use dope::EventRef;
use dope::driver::token::Token;
use dope::hash;
use dope::io::provided::ProvidedView;
use dope::manifold::env::Bundle;
use dope::manifold::listener::application::Application;
use dope::manifold::listener::config::Config;
use dope::manifold::typed::TypedToken;
use dope::manifold::{Manifold, Outcome, listener};
use dope::runtime::dispatcher::Idle;
use dope::runtime::profile::Balanced;
use dope_net::Transport;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;

use super::port::Port;
use super::port::recv::arena::{RecvArena, RecvLayout};
use dope::Event;
use dope::manifold::listener::state::{Aux, State};
use dope::runtime::executor::StorageFactory;

pub struct ListenerPort<'d> {
    connections: Port<'d>,
    accepts: CellQueue<Token>,
    waiters: Pin<Box<WaitQueue>>,
}

pub struct ListenerPortFactory {
    layout: RecvLayout,
}

impl<'d> ListenerPort<'d> {
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

struct AcceptQueue<'scope, 'd, W: Wire> {
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
        if self
            .get_mut()
            .port
            .connections
            .push_recv(slot.token(), chunk)
        {
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
        if self
            .get_mut()
            .port
            .connections
            .push_retained(slot.token(), chunk)
        {
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
        self.get_mut().port.connections.closed(slot.token());
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
    listener::Listener<'d, ID, AcceptQueue<'scope, 'd, W>, Bundle<T, W, Balanced>>;

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
        let inner = listener::Listener::open_in(
            AcceptQueue::<W> {
                port,
                _wire: PhantomData,
            },
            Config {
                max_connections: port.connections.capacity(),
                bind: addr.clone(),
                backlog,
                stream: stream_config,
                transport: listener_config,
                egress: Default::default(),
            },
            hash_builder,
            driver,
        )?;
        Ok(Self { inner, port })
    }

    fn sync_send(inner: Pin<&Inner<'scope, 'd, ID, T, W>>, port: &ListenerPort<'d>, conn: Token) {
        let pending = inner.has_pending_egress(conn);
        port.connections.sync_send(conn, pending);
    }

    fn apply_requests(mut self: Pin<&mut Self>, conn: Token, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        let requests = port.connections.requests(conn);
        // SAFETY: the deferred queue contains only tokens emitted by `inner`,
        // whose route is the const `ID` carried by this wrapper.
        let typed = unsafe { TypedToken::<Inner<'scope, 'd, ID, T, W>>::new_unchecked(conn) };
        if let Some(requests) = requests {
            let inner = self.as_ref().project_ref().inner.get_ref();
            if let Some(bytes) = requests.send
                && !inner.mark_send(conn, bytes)
            {
                port.connections.failed(conn);
            }
            if requests.close {
                inner.close(conn);
            }
        }
        Manifold::activate(self.as_mut().project().inner, typed, driver);
        Self::sync_send(self.as_ref().project_ref().inner, port, conn);
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
        Manifold::dispatch(self.as_mut().project().inner, ev, driver);
        if let Some(conn) = conn {
            Self::sync_send(self.as_ref().project_ref().inner, port, conn);
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let port = self.as_ref().get_ref().port;
        while let Some(conn) = port.connections.pop_deferred_request() {
            self.as_mut().apply_requests(conn, driver);
        }
        Manifold::pre_park(self.project().inner, driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.apply_requests(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Manifold::idle(self.project_ref().inner)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::shutdown(self.project().inner, driver);
    }
}

#[pin_project]
pub struct Accept<'scope, 'd> {
    host: ListenerHandle<'scope, 'd>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'scope, 'd> Fiber<'d> for Accept<'scope, 'd> {
    type Output = io::Result<Io<'scope, 'd>>;

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
