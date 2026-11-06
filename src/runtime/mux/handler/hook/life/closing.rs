use crate::runtime::mux::handler::hook::Boundary;
use o3::buffer;
use std::pin;

use crate::conn::{id, life};
use crate::runtime::mux::handler;
use crate::stream;
use handler::{Handler, Slot};

pub struct Event<'scope, H, const DOMAIN: u8, B, D, I>
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    handler: pin::Pin<&'scope mut H>,
    slot: Slot<'scope, H::Connection, DOMAIN, B, D>,
    handle: id::Handle<I, DOMAIN>,
}

impl<'scope, H, const DOMAIN: u8, B, D, I> Event<'scope, H, DOMAIN, B, D, I>
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    /// Moves reconnect ownership while terminal packet protection stays retained.
    pub fn into_saved(mut self) -> (Self, Option<life::Reconnect>) {
        let saved = self.slot.transport().security().take_final();
        (self, saved)
    }
}

impl<'scope, H, const DOMAIN: u8, B, D, I> From<Event<'scope, H, DOMAIN, B, D, I>>
    for (
        pin::Pin<&'scope mut H>,
        Slot<'scope, H::Connection, DOMAIN, B, D>,
        id::Handle<I, DOMAIN>,
    )
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn from(value: Event<'scope, H, DOMAIN, B, D, I>) -> Self {
        (value.handler, value.slot, value.handle)
    }
}

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn closing<'scope>(event: Event<'scope, Self, DOMAIN, B, D, I>);
}

pub struct On;
pub struct Off;

pub trait Mode<H, const DOMAIN: u8, B, D, I>: Boundary
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn notify<'scope>(
        handler: pin::Pin<&'scope mut H>,
        slot: Slot<'scope, H::Connection, DOMAIN, B, D>,
        handle: id::Handle<I, DOMAIN>,
    );
}

impl<H, const DOMAIN: u8, B, D, I> Mode<H, DOMAIN, B, D, I> for On
where
    H: Sink<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn notify<'scope>(
        handler: pin::Pin<&'scope mut H>,
        slot: Slot<'scope, H::Connection, DOMAIN, B, D>,
        handle: id::Handle<I, DOMAIN>,
    ) {
        H::closing(Event {
            handler,
            slot,
            handle,
        });
    }
}

impl<H, const DOMAIN: u8, B, D, I> Mode<H, DOMAIN, B, D, I> for Off
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn notify<'scope>(
        _: pin::Pin<&'scope mut H>,
        _: Slot<'scope, H::Connection, DOMAIN, B, D>,
        _: id::Handle<I, DOMAIN>,
    ) {
    }
}
