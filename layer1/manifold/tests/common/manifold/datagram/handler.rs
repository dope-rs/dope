use std::{net::SocketAddr, pin::Pin};

use dope_core::{
    driver::{
        retained,
        route::KeyTag,
        schedule::{self, app, clock},
    },
    io::datagram::ReceiveError,
};
use dope_manifold::network::datagram::{
    self, Socket,
    hook::{self, ingress},
    packet::Packet,
    send,
};
use o3::{base::cell::region, buffer::pool};

pub enum Event<'socket, 'access, 'work, 'turn, 'd, M: send::Mode = send::Plain> {
    Packet(Result<(SocketAddr, Packet<'turn, 'd>), ReceiveError>),
    Access(
        Result<(SocketAddr, Packet<'access, 'd>), ReceiveError>,
        Pin<&'access mut Socket<'d, KeyTag<0>, pool::Cursor, M>>,
    ),
    Park(
        Pin<&'socket mut Socket<'d, KeyTag<0>, pool::Cursor, M>>,
        clock::Moment<'d>,
        app::Work<'work, 'd>,
    ),
    Settle(send::Settlement<'turn, 'd, KeyTag<0>>),
    Transfer(send::Transfer<'d, KeyTag<0>>),
}

pub trait Progress {
    fn is_runnable(&self) -> bool;
}

pub trait State {
    type Hooks: hook::Bundle;
    type Mode;
}

pub struct Handler<S, F> {
    state: S,
    callback: F,
}

impl<S, F> Handler<S, F> {
    pub fn new(state: S, callback: F) -> Self {
        Self { state, callback }
    }
}

impl<'d, S, F> datagram::Ingress<'d, KeyTag<0>> for Handler<S, F>
where
    S: State,
    S::Mode: ingress::Failure<'d, KeyTag<0>, Self>,
{
    type Hooks = S::Hooks;
    type Mode = S::Mode;
}

impl<'d, S, F, M: send::Mode> datagram::Access<'d, KeyTag<0>> for Handler<S, F>
where
    S: State<Mode = ingress::Access> + Unpin,
    S::Hooks: hook::Bundle<Buffer = pool::Cursor, Send = M>,
    F: for<'socket, 'access, 'work, 'turn> FnMut(
            &mut S,
            Event<'socket, 'access, 'work, 'turn, 'd, M>,
        ) + Unpin,
{
    fn packet<'turn>(
        self: Pin<&mut Self>,
        received: Result<(SocketAddr, Packet<'turn, 'd>), ReceiveError>,
        socket: Pin<&'turn mut Socket<'d, KeyTag<0>, pool::Cursor, M>>,
    ) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, Event::Access(received, socket));
    }
}

impl<'d, S, F> datagram::Time<'d, KeyTag<0>> for Handler<S, F>
where
    S: State<Mode = ingress::Time> + Unpin,
    F: for<'socket, 'access, 'work, 'turn> FnMut(&mut S, Event<'socket, 'access, 'work, 'turn, 'd>)
        + Unpin,
{
    fn packet<'turn>(
        self: Pin<&mut Self>,
        received: Result<(SocketAddr, Packet<'turn, 'd>), ReceiveError>,
        _: clock::Moment<'d>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, Event::Packet(received));
    }
}

impl<'d, S, F, M: send::Mode> datagram::Application<'d, KeyTag<0>> for Handler<S, F>
where
    S: State + Progress + Unpin,
    S::Hooks: hook::Bundle<Buffer = pool::Cursor, Send = M>,
    S::Mode: ingress::Failure<'d, KeyTag<0>, Self>,
    F: for<'socket, 'access, 'work, 'turn> FnMut(
            &mut S,
            Event<'socket, 'access, 'work, 'turn, 'd, M>,
        ) + Unpin,
{
    fn pre_park<'turn>(
        self: Pin<&mut Self>,
        socket: Pin<&mut Socket<'d, KeyTag<0>, pool::Cursor, M>>,
        now: clock::Moment<'d>,
        work: app::Work<'turn, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, Event::Park(socket, now, work));
    }

    fn progress(
        self: Pin<&Self>,
        _: clock::Clock<'_, 'd>,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        if self.get_ref().state.is_runnable() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
}

impl<'d, S, F> datagram::Settlement<'d, KeyTag<0>> for Handler<S, F>
where
    S: State + Unpin,
    S::Hooks: hook::Bundle<Buffer = pool::Cursor, Send = send::Plain>,
    S::Mode: ingress::Failure<'d, KeyTag<0>, Self>,
    F: for<'socket, 'access, 'work, 'turn> FnMut(&mut S, Event<'socket, 'access, 'work, 'turn, 'd>)
        + Unpin,
{
    fn settle<'turn>(
        self: Pin<&mut Self>,
        event: send::Settlement<'turn, 'd, KeyTag<0>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, Event::Settle(event));
    }
}

impl<'d, S, F> datagram::Transfer<'d, KeyTag<0>> for Handler<S, F>
where
    S: Unpin,
    F: for<'socket, 'access, 'work, 'turn> FnMut(&mut S, Event<'socket, 'access, 'work, 'turn, 'd>)
        + Unpin,
{
    fn transferred(self: Pin<&mut Self>, transfer: send::Transfer<'d, KeyTag<0>>) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, Event::Transfer(transfer));
    }
}

impl<S: datagram::Shutdown + Unpin, F: Unpin> datagram::Shutdown for Handler<S, F> {
    fn shutdown(self: Pin<&mut Self>) {
        Pin::new(&mut self.get_mut().state).shutdown();
    }
}
