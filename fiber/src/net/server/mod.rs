use std::{io, pin, task, time};

use dope::{
    core::driver::{self, retained, schedule},
    manifold::{
        self,
        listener::{self, config, connection, handler},
        receive, timing,
    },
    runtime::random,
};
use o3::collections::{self, queue::slot};
use transport::wire;

use crate::{
    abi, context,
    net::{
        self,
        port::{self, recv::arena},
    },
    wait,
};

mod factory;
mod sealed;

pub struct ListenerPort<'d, W: wire::Wire, const ID: u8 = 0> {
    connections: port::Table<'d, W::RetainedRecv<'d>, connection::Id<'d, ID>>,
    accepts: Accepts<'d, ID>,
    waiters: wait::Queue<'d>,
    wire_storage: W::ConnectionStorage<ID>,
}

struct Accepts<'d, const ID: u8> {
    queued: slot::Cell<connection::Id<'d, ID>>,
}

impl<'d, const ID: u8> Accepts<'d, ID> {
    fn try_with_capacity(capacity: usize) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            queued: slot::Cell::try_with_capacity(capacity)?,
        })
    }

    /// Installs the newest generation for a connection slot at the back of
    /// the queue. The indexed queue removes an older generation in O(1).
    fn replace_back(&self, accepted: connection::Id<'d, ID>) -> bool {
        let index = accepted.index();
        let _stale = self.queued.remove(index);
        self.queued.push_back(index, accepted).is_ok()
    }

    fn pop_front(&self) -> Option<connection::Id<'d, ID>> {
        self.queued.pop_front()
    }
}

pub struct ListenerPortFactory<W: wire::Wire, const ID: u8 = 0> {
    layout: arena::RecvLayout,
    wire_storage: W::ConnectionStorage<ID>,
}

impl<'d, W: wire::Wire, const ID: u8> ListenerPort<'d, W, ID> {
    pub fn factory(capacity: usize) -> io::Result<ListenerPortFactory<W, ID>> {
        let layout = arena::RecvLayout::new(capacity)?;
        let wire_storage = W::connection_storage::<ID>(layout.connections())?;
        Ok(ListenerPortFactory {
            layout,
            wire_storage,
        })
    }

    fn activate(
        &self,
        accepted: connection::Id<'d, ID>,
        work: schedule::Application<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        if self.waiters.wake_one(work) == wait::WakeStatus::Pending {
            return false;
        }
        if !self
            .connections
            .activate_deferred(accepted, driver.region_token())
        {
            return false;
        }
        if !self.accepts.replace_back(accepted) {
            return false;
        }
        true
    }

    pub fn handle(&self) -> ListenerHandle<'_, 'd, W, ID> {
        ListenerHandle { port: self }
    }

    pub fn wire_storage(&self) -> &W::ConnectionStorage<ID> {
        &self.wire_storage
    }
}

struct AcceptQueue<'scope, 'd, W: wire::Wire, const ID: u8> {
    port: &'scope ListenerPort<'d, W, ID>,
}

impl<'scope, 'd, W: wire::Wire, const ID: u8> AcceptQueue<'scope, 'd, W, ID> {
    fn sync_send(&self, connection: &connection::Ctx<'_, 'd, ID, W, ()>) {
        let id = connection.id();
        let pending = connection.has_pending_egress();
        self.port.connections.channel().sync_send(id, pending);
    }
}

impl<'scope, 'd, W: wire::Wire, const ID: u8> handler::Application<'d, ID>
    for AcceptQueue<'scope, 'd, W, ID>
{
    type Conn = ();
    type Wire = W;
    type Input = receive::Retained;

    fn deadline(self: pin::Pin<&Self>) -> Option<time::Instant> {
        None
    }

    fn send(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, W, ()>,
        _sent: usize,
        _driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.as_ref().get_ref().sync_send(&connection);
    }

    fn activate(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, W, ()>,
        _driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.as_ref().get_ref().sync_send(&connection);
    }

    fn close(self: pin::Pin<&mut Self>, connection: connection::Ctx<'_, 'd, ID, W, ()>) {
        self.get_mut()
            .port
            .connections
            .channel()
            .closed(connection.id());
    }

    fn defer_close(self: pin::Pin<&Self>, connection: connection::Ref<'_, 'd, ID, W, ()>) -> bool {
        !self
            .get_ref()
            .port
            .connections
            .channel()
            .retained_drained(connection.id())
    }

    fn accept(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, W, ()>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> manifold::Outcome {
        let work = connection.application_work();
        let port = self.get_mut().port;
        if port.activate(connection.id(), work, driver) {
            manifold::Outcome::Ok
        } else {
            manifold::Outcome::Overrun
        }
    }
}

impl<'scope, 'd, W: wire::Wire, const ID: u8> handler::RetainedApplication<'d, ID>
    for AcceptQueue<'scope, 'd, W, ID>
{
    const RETENTION: receive::Retention = arena::RecvLayout::RETENTION;

    fn retained_chunk(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, W, ()>,
        chunk: W::RetainedRecv<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> manifold::Outcome {
        if self.get_mut().port.connections.channel().push_retained(
            connection.id(),
            chunk,
            driver.region_token(),
        ) {
            manifold::Outcome::Overrun
        } else {
            manifold::Outcome::Ok
        }
    }
}

type Inner<'scope, 'd, const ID: u8, T, W> = listener::Listener<
    'd,
    ID,
    AcceptQueue<'scope, 'd, W, ID>,
    manifold::Bundle<T, W, timing::Balanced>,
>;

#[pin_project::pin_project(!Unpin)]
pub struct Listener<'scope, 'd, const ID: u8, T: transport::Transport, W: wire::Wire> {
    #[pin]
    inner: Inner<'scope, 'd, ID, T, W>,
    port: &'scope ListenerPort<'d, W, ID>,
}

impl<'scope, 'd: 'scope, const ID: u8, T: transport::Transport, W: wire::Wire>
    Listener<'scope, 'd, ID, T, W>
{
    pub fn bind(
        port: &'scope ListenerPort<'d, W, ID>,
        driver: &mut driver::Context<'_, 'd>,
        addr: T::Addr,
        backlog: i32,
        listener_config: T::ListenerConfig,
        stream_config: T::StreamConfig,
        hash_builder: random::HashState<'d>,
    ) -> io::Result<Self>
    where
        T: transport::ListenerTransport,
        W::InitConfig<'d, ID>: Default,
    {
        let config = config::Config {
            max_connections: port.connections.capacity(),
            direct_flights: 0,
            bind: addr,
            backlog,
            stream: stream_config,
            transport: listener_config,
            egress: Default::default(),
        };
        Self::bind_with_wire(
            port,
            driver,
            config,
            W::InitConfig::<'d, ID>::default(),
            hash_builder,
        )
    }

    pub fn bind_with_wire(
        port: &'scope ListenerPort<'d, W, ID>,
        driver: &mut driver::Context<'_, 'd>,
        mut config: config::Config<T>,
        wire_config: W::InitConfig<'d, ID>,
        hash_builder: random::HashState<'d>,
    ) -> io::Result<Self>
    where
        T: transport::ListenerTransport,
    {
        config.max_connections = port.connections.capacity();
        let inner = listener::Listener::open_in_with_wire(
            AcceptQueue::<W, ID> { port },
            config,
            wire_config,
            hash_builder,
            driver,
        )?;
        Ok(Self { inner, port })
    }
}

pub struct ListenerHandle<'scope, 'd, W: wire::Wire, const ID: u8 = 0> {
    port: &'scope ListenerPort<'d, W, ID>,
}

impl<W: wire::Wire, const ID: u8> Copy for ListenerHandle<'_, '_, W, ID> {}

impl<W: wire::Wire, const ID: u8> Clone for ListenerHandle<'_, '_, W, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'd, W: wire::Wire, const ID: u8> ListenerHandle<'scope, 'd, W, ID> {
    pub fn accept(self) -> Accept<'scope, 'd, W, ID> {
        Accept {
            host: self,
            waiter: wait::Waiter::new(),
        }
    }
}

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Accept<'scope, 'd, W: wire::Wire, const ID: u8 = 0> {
    host: ListenerHandle<'scope, 'd, W, ID>,
    #[pin]
    waiter: wait::Waiter<'scope, 'd>,
}

impl<'scope, 'd, W: wire::Wire, const ID: u8> abi::Fiber<'d> for Accept<'scope, 'd, W, ID> {
    type Output = io::Result<net::Io<'scope, 'd, W, connection::Id<'d, ID>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.project();
        let waiter = this.waiter;
        use std::task::Poll;
        let Some(accepted) = this.host.port.accepts.pop_front() else {
            return if this
                .host
                .port
                .waiters
                .try_register(waiter.as_ref(), cx.as_ref())
            {
                Poll::Pending
            } else {
                Poll::Ready(Err(io::Error::other("listener waiter capacity exhausted")))
            };
        };
        waiter.as_ref().unregister();
        Poll::Ready(Ok(net::Io::new(&this.host.port.connections, accepted)))
    }
}
