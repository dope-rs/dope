mod connect;

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Waker;

pub(super) use connect::Connect;
use o3::buffer::Shared;

use super::pending::{Pending, Resolve};
use super::state::{RecvInto, SendIdle, State};
use super::{ConnEnv, Holding};
use crate::fiber::io::Host;
use crate::manifold::connector::source::Explicit;
use crate::manifold::connector::{ChunkOutcome, ConnApp, Core, session};
use crate::transport::Transport;
use crate::transport::link::Slot;
use crate::transport::wire::{RecvChunk, Wire};
use crate::{Driver, backend};

enum Resolved {
    Ready(backend::token::Token),
    Failed,
}

pub(super) enum ConnectOutcome {
    Conn(backend::token::Token),
    Failed(io::Error),
    Pending,
}

pub struct AsyncApp<W: Wire> {
    pending: Pending<Resolved>,
    _w: PhantomData<fn() -> W>,
}

impl<W: Wire> Default for AsyncApp<W> {
    fn default() -> Self {
        Self {
            pending: Pending::default(),
            _w: PhantomData,
        }
    }
}

impl<W: Wire> AsyncApp<W> {
    fn resolve(&mut self, tag: u32, waker: &Waker) -> ConnectOutcome {
        match self.pending.poll(tag, waker) {
            Resolve::Ready(Resolved::Ready(token)) => ConnectOutcome::Conn(token),
            Resolve::Ready(Resolved::Failed) => {
                ConnectOutcome::Failed(io::Error::other("fiber::Connector: connect failed"))
            }
            Resolve::Pending => ConnectOutcome::Pending,
        }
    }

    fn cancel(&mut self, tag: u32) {
        self.pending.cancel(tag);
    }
}

impl<W: Wire> ConnApp for AsyncApp<W> {
    type Conn = State;
    type Wire = W;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, session::State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        _driver: &mut Driver,
    ) -> ChunkOutcome {
        if slot.state.conn.push_recv(chunk.as_slice()) {
            ChunkOutcome::Overrun
        } else {
            ChunkOutcome::Ok
        }
    }

    fn on_connected(
        &mut self,
        tag: u32,
        slot: &mut Slot<Self::Wire, session::State<Self::Conn>>,
        _driver: &mut Driver,
    ) {
        let token = slot.token();
        self.pending.settle(tag, Resolved::Ready(token));
    }

    fn on_connect_failed(&mut self, tag: u32, _driver: &mut Driver) {
        self.pending.settle(tag, Resolved::Failed);
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<Self::Wire, session::State<Self::Conn>>,
        _sent: usize,
        _driver: &mut Driver,
    ) {
        if slot.state.egress_len() == 0 {
            slot.state.conn.wake_send();
        }
    }

    fn on_close(&mut self, slot: &mut Slot<Self::Wire, session::State<Self::Conn>>) {
        slot.state.conn.signal_closed();
    }
}

pub type Connector<const ID: u8, T, W> = Core<ID, AsyncApp<W>, Explicit<T>, ConnEnv<T, W>>;

impl<const ID: u8, T: Transport, W: Wire> Core<ID, AsyncApp<W>, Explicit<T>, ConnEnv<T, W>>
where
    T::Addr: Clone,
{
    pub fn new(capacity: usize, driver: &mut Driver) -> Self {
        Self::with_app(AsyncApp::default(), Explicit::default(), capacity, driver)
    }

    pub fn connect_held<'d>(
        this: Holding<'d, Self>,
        addr: T::Addr,
        opts: T::StreamOpts,
    ) -> super::Fiber<'d, Connect<'d, ID, T, W>> {
        super::Fiber::new(Connect::new(this, addr, opts))
    }

    pub(super) fn try_connect(
        mut self: Pin<&mut Self>,
        addr: T::Addr,
        opts: T::StreamOpts,
    ) -> Option<u32> {
        self.as_mut().set_stream_opts(opts);
        self.enqueue_dial(addr)
    }

    pub(super) fn try_resolve(self: Pin<&mut Self>, tag: u32, w: &Waker) -> ConnectOutcome {
        self.app_mut().resolve(tag, w)
    }

    pub(super) fn cancel_connect(mut self: Pin<&mut Self>, tag: u32) {
        self.as_mut().app_mut().cancel(tag);
        self.cancel_dial(tag);
    }
}

impl<const ID: u8, T: Transport, W: Wire> Host for Core<ID, AsyncApp<W>, Explicit<T>, ConnEnv<T, W>>
where
    T::Addr: Clone,
{
    fn recv_into(self: Pin<&mut Self>, id: backend::token::Token, dst: &mut [u8]) -> RecvInto {
        self.state_for(id)
            .map_or(RecvInto::Bytes(0), |s| s.conn.try_recv_into(dst))
    }

    fn recv_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker) {
        if let Some(s) = self.state_for(id) {
            s.conn.set_recv_waker(w);
        }
    }

    fn send_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker) {
        if let Some(s) = self.state_for(id) {
            s.conn.set_send_waker(w);
        }
    }

    fn send(mut self: Pin<&mut Self>, id: backend::token::Token, bytes: Shared) {
        if let Some(s) = self.as_mut().state_for(id)
            && !s.enqueue(bytes)
        {
            s.conn.signal_error(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "fiber: egress queue over cap",
            ));
        }
        self.request_flush(id);
    }

    fn send_idle(self: Pin<&mut Self>, id: backend::token::Token) -> SendIdle {
        self.state_for(id).map_or(SendIdle::Idle, |s| {
            let inflight = s.egress_len() != 0;
            s.conn.send_status(inflight)
        })
    }

    fn shutdown(self: Pin<&mut Self>, id: backend::token::Token, how: i32) {
        self.shutdown_conn(id, how);
    }

    fn close(self: Pin<&mut Self>, id: backend::token::Token) {
        self.request_close(id);
    }
}
