use std::{ops, pin};

use dope_core::{
    driver::{route::Identity, schedule::clock},
    io::{self, datagram::ReceiveError},
};
use o3::base::cell::region;

use crate::{
    app::dispatch::{self, event},
    network::datagram::{self, hook},
};

pub struct Time;
pub struct Access;
pub struct Coupled;

pub trait Mode<R> {
    type Dispatch;
}

impl Mode<hook::Disabled> for Access {
    type Dispatch = event::Observe;
}

impl Mode<hook::Enabled> for Time {
    type Dispatch = event::Driven<dispatch::raw::Plain>;
}

impl Mode<hook::Enabled> for Coupled {
    type Dispatch = event::Driven<dispatch::raw::Plain>;
}

pub trait Failure<'d, ID: Identity, H>
where
    H: datagram::Ingress<'d, ID>,
{
    fn failed(
        endpoint: pin::Pin<&mut hook::Endpoint<'d, ID, H>>,
        error: ReceiveError,
        now: clock::Moment<'d>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );
}

impl<'d, ID: Identity, H: datagram::Access<'d, ID>> Failure<'d, ID, H> for Access {
    fn failed(
        endpoint: pin::Pin<&mut hook::Endpoint<'d, ID, H>>,
        error: ReceiveError,
        _: clock::Moment<'d>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = endpoint.project();
        this.handler.packet(Err(error), this.socket);
    }
}

impl<'d, ID: Identity, H: datagram::Time<'d, ID>> Failure<'d, ID, H> for Time {
    fn failed(
        endpoint: pin::Pin<&mut hook::Endpoint<'d, ID, H>>,
        error: ReceiveError,
        now: clock::Moment<'d>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        endpoint.project().handler.packet(Err(error), now, token);
    }
}

impl<'d, ID: Identity, H: datagram::Coupled<'d, ID>> Failure<'d, ID, H> for Coupled {
    fn failed(
        endpoint: pin::Pin<&mut hook::Endpoint<'d, ID, H>>,
        error: ReceiveError,
        now: clock::Moment<'d>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = endpoint.project();
        this.handler.packet(Err(error), this.socket, now);
    }
}

pub(in crate::network::datagram) trait Drive<'d, ID: Identity, H, R>
where
    H: datagram::Ingress<'d, ID>,
{
    fn dispatch(
        socket: pin::Pin<&mut hook::Socket<'d, ID, H::Hooks>>,
        event: io::Event<'d>,
        sink: pin::Pin<&mut H>,
        clock: clock::Clock<'_, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> ops::ControlFlow<io::Event<'d>>;
}

impl<'d, ID: Identity, H> Drive<'d, ID, H, hook::Enabled> for Time
where
    H: datagram::Time<'d, ID> + datagram::Settlement<'d, ID>,
{
    fn dispatch(
        mut socket: pin::Pin<&mut hook::Socket<'d, ID, H::Hooks>>,
        event: io::Event<'d>,
        mut sink: pin::Pin<&mut H>,
        clock: clock::Clock<'_, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let now = clock.now();
        let event = match socket.as_mut().project().receive.route(event) {
            ops::ControlFlow::Continue(packet) => {
                packet.dispatch(|packet| {
                    sink.as_mut().packet(packet, now, token);
                });
                return ops::ControlFlow::Continue(());
            }
            ops::ControlFlow::Break(event) => event,
        };
        socket.settle(token, event, sink, now)
    }
}

impl<'d, ID: Identity, H> Drive<'d, ID, H, hook::Enabled> for Coupled
where
    H: datagram::Coupled<'d, ID> + datagram::Settlement<'d, ID>,
{
    fn dispatch(
        mut socket: pin::Pin<&mut hook::Socket<'d, ID, H::Hooks>>,
        event: io::Event<'d>,
        mut sink: pin::Pin<&mut H>,
        clock: clock::Clock<'_, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let now = clock.now();
        let event = match socket.as_mut().project().receive.route(event) {
            ops::ControlFlow::Continue(packet) => {
                packet.dispatch(|packet| {
                    sink.as_mut().packet(packet, socket.as_mut(), now);
                });
                return ops::ControlFlow::Continue(());
            }
            ops::ControlFlow::Break(event) => event,
        };
        socket.settle(token, event, sink, now)
    }
}
