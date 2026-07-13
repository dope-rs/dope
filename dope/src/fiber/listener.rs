use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use o3::buffer::Shared;
use pin_project::pin_project;

use super::io::{Host, Io};
use super::state::{RecvInto, SendIdle, State};
use super::{ConnEnv, Holding};
use crate::manifold::listener::{Application, Config};
use crate::manifold::{Manifold, Outcome, listener};
use crate::transport::Transport;
use crate::transport::link::Slot;
use crate::transport::wire::{RecvChunk, Wire};
use crate::{Driver, backend};

pub(super) struct AcceptQueue<W: Wire> {
    accepts: VecDeque<backend::token::Token>,
    waker: Option<backend::park::WakeRef>,
    cap: usize,
    _w: PhantomData<fn() -> W>,
}

impl<W: Wire> AcceptQueue<W> {
    pub(super) fn with_cap(cap: usize) -> Self {
        Self {
            accepts: VecDeque::new(),
            waker: None,
            cap,
            _w: PhantomData,
        }
    }

    pub(super) fn pop(&mut self) -> Option<backend::token::Token> {
        self.accepts.pop_front()
    }

    pub(super) fn arm(&mut self, w: &Waker) {
        self.waker = Some(backend::park::WakeRef::verified(w));
    }
}

impl<W: Wire> Application for AcceptQueue<W> {
    type Conn = State;
    type Wire = W;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<State>>,
        chunk: RecvChunk<'_>,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) -> Outcome {
        if slot.state.conn.push_recv(chunk.as_slice()) {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn on_send<'d>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<State>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) {
        slot.state.conn.wake_send();
    }

    fn on_close<'d>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<State>>,
        _aux: &mut listener::Aux,
    ) {
        slot.state.conn.signal_closed();
    }

    fn on_accept<'d>(
        &mut self,
        slot: &mut Slot<'d, W, listener::State<State>>,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) -> Outcome {
        if self.accepts.len() >= self.cap {
            return Outcome::Overrun;
        }
        self.accepts.push_back(slot.token());
        if let Some(w) = self.waker.take() {
            w.wake();
        }
        Outcome::Ok
    }
}

type Inner<'d, const ID: u8, T, W> = listener::Listener<'d, ID, AcceptQueue<W>, ConnEnv<T, W>>;

#[pin_project(!Unpin)]
pub struct Listener<'d, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    #[pin]
    inner: Inner<'d, ID, T, W>,
}

impl<'d, const ID: u8, T: Transport, W: Wire> Listener<'d, ID, T, W>
where
    T::Addr: Clone,
{
    pub fn bind(
        capacity: usize,
        driver: &'d Driver,
        addr: &T::Addr,
        backlog: i32,
        listener_opts: T::ListenerOpts,
        stream_opts: T::StreamOpts,
    ) -> io::Result<Self> {
        let inner = listener::Listener::open_in(
            AcceptQueue::<W>::with_cap(capacity),
            Self::config(capacity, addr, backlog, listener_opts, stream_opts),
            driver,
        )?;
        Ok(Self { inner })
    }

    fn config(
        capacity: usize,
        addr: &T::Addr,
        backlog: i32,
        listener_opts: T::ListenerOpts,
        stream_opts: T::StreamOpts,
    ) -> Config<T> {
        Config {
            max_conn: capacity,
            bind: addr.clone(),
            backlog,
            stream_opts,
            listener_opts,
        }
    }

    pub fn accept_held<'h>(this: Holding<'h, Self>) -> super::Fiber<'h, Accept<'h, 'd, ID, T, W>> {
        super::Fiber::new(Accept { host: this })
    }
}

impl<'d, const ID: u8, T: Transport, W: Wire> Host for Listener<'d, ID, T, W>
where
    T::Addr: Clone,
{
    fn recv_into(self: Pin<&mut Self>, id: backend::token::Token, dst: &mut [u8]) -> RecvInto {
        let Some(view) = self.project().inner.conn_view_mut(id) else {
            return RecvInto::Bytes(0);
        };
        view.state.try_recv_into(dst)
    }

    fn recv_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker) {
        let Some(view) = self.project().inner.conn_view_mut(id) else {
            return;
        };
        view.state.set_recv_waker(w);
    }

    fn send_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker) {
        let Some(view) = self.project().inner.conn_view_mut(id) else {
            return;
        };
        view.state.set_send_waker(w);
    }

    fn send(self: Pin<&mut Self>, id: backend::token::Token, bytes: Shared) {
        let mut inner = self.project().inner;
        if inner.as_mut().mark_send(id, bytes) {
            return;
        }
        if let Some(view) = inner.conn_view_mut(id) {
            view.state.signal_error(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "fiber: egress queue over cap",
            ));
        }
    }

    fn send_idle(self: Pin<&mut Self>, id: backend::token::Token) -> SendIdle {
        let Some(view) = self.project().inner.conn_view_mut(id) else {
            return SendIdle::Idle;
        };
        view.state.send_status(view.inflight)
    }

    fn shutdown(self: Pin<&mut Self>, id: backend::token::Token, how: i32) {
        self.project().inner.shutdown(id, how);
    }

    fn close(self: Pin<&mut Self>, id: backend::token::Token) {
        self.project().inner.close(id);
    }
}

impl<'d, const ID: u8, T: Transport, W: Wire> Manifold<'d> for Listener<'d, ID, T, W>
where
    T::Addr: Clone,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: backend::Event, driver: &'d Driver) {
        Manifold::dispatch(self.project().inner, ev, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver) {
        self.project().inner.pre_park(driver);
    }

    fn on_wake(
        self: Pin<&mut Self>,
        target: crate::manifold::route::TypedToken<Self>,
        driver: &'d Driver,
    ) {
        // SAFETY: fiber Listener<ID, T, W> shares const ID with Inner<'d, ID, T, W>; TypedToken<Self> bits match Inner's Manifold::ID.
        let inner = unsafe {
            crate::manifold::route::TypedToken::<Inner<'d, ID, T, W>>::from_raw_token(
                target.token(),
            )
        };
        Manifold::on_wake(self.project().inner, inner, driver);
    }

    fn idle(self: Pin<&Self>) -> crate::runtime::dispatcher::Idle {
        Manifold::idle(self.project_ref().inner)
    }

    fn on_shutdown(self: Pin<&mut Self>, driver: &'d Driver) {
        Manifold::on_shutdown(self.project().inner, driver);
    }
}

pub struct Accept<'h, 'd, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    host: Holding<'h, Listener<'d, ID, T, W>>,
}

impl<const ID: u8, T: Transport, W: Wire> Unpin for Accept<'_, '_, ID, T, W> where T::Addr: Clone {}

impl<'h, 'd, const ID: u8, T: Transport, W: Wire> Future for Accept<'h, 'd, ID, T, W>
where
    T::Addr: Clone,
{
    type Output = io::Result<Io<'h, Listener<'d, ID, T, W>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let host_clone = this.host;
        let mut h = this.host.hold();
        loop {
            let Some(id) = h.as_mut().project().inner.handler_mut_pin().pop() else {
                h.as_mut().project().inner.handler_mut_pin().arm(cx.waker());
                return Poll::Pending;
            };
            if h.as_mut().project().inner.conn_view_mut(id).is_some() {
                return Poll::Ready(Ok(Io::new(host_clone, id)));
            }
        }
    }
}
