mod connect;

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Waker;

pub(super) use connect::Connect;
use o3::buffer::Shared;

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
    inbox: Vec<(u32, Resolved)>,
    waiters: Vec<(u32, backend::park::WakeRef)>,
    _w: PhantomData<fn() -> W>,
}

impl<W: Wire> Default for AsyncApp<W> {
    fn default() -> Self {
        Self {
            inbox: Vec::new(),
            waiters: Vec::new(),
            _w: PhantomData,
        }
    }
}

impl<W: Wire> AsyncApp<W> {
    fn wake(&mut self, tag: u32) {
        if let Some(pos) = self.waiters.iter().position(|(t, _)| *t == tag) {
            let (_, w) = self.waiters.swap_remove(pos);
            w.wake();
        }
    }

    fn resolve(&mut self, tag: u32, waker: &Waker) -> ConnectOutcome {
        if let Some(pos) = self.inbox.iter().position(|(t, _)| *t == tag) {
            let (_, r) = self.inbox.swap_remove(pos);
            return match r {
                Resolved::Ready(token) => ConnectOutcome::Conn(token),
                Resolved::Failed => {
                    ConnectOutcome::Failed(io::Error::other("fiber::Connector: connect failed"))
                }
            };
        }
        let wref = backend::park::WakeRef::verified(waker);
        match self.waiters.iter_mut().find(|(t, _)| *t == tag) {
            Some(slot) => slot.1 = wref,
            None => self.waiters.push((tag, wref)),
        }
        ConnectOutcome::Pending
    }

    fn cancel(&mut self, tag: u32) {
        self.waiters.retain(|(t, _)| *t != tag);
        self.inbox.retain(|(t, _)| *t != tag);
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
        self.inbox.push((tag, Resolved::Ready(token)));
        self.wake(tag);
    }

    fn on_connect_failed(&mut self, tag: u32, _driver: &mut Driver) {
        self.inbox.push((tag, Resolved::Failed));
        self.wake(tag);
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
