use o3::buffer;
use std::pin;

use crate::conn::id;
use crate::runtime::mux::handler::{Handler, Slot};
use crate::runtime::routing::registry::Notification;
use crate::stream;

type Observation<I> = <I as id::Temporal>::Observation;

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
    at: Observation<I>,
    notified: &'scope mut Notification,
}

impl<'scope, H, const DOMAIN: u8, B, D, I> Event<'scope, H, DOMAIN, B, D, I>
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    pub(in crate::runtime) fn new(
        handler: pin::Pin<&'scope mut H>,
        slot: Slot<'scope, H::Connection, DOMAIN, B, D>,
        handle: id::Handle<I, DOMAIN>,
        at: Observation<I>,
        notified: &'scope mut Notification,
    ) -> Self {
        Self {
            handler,
            slot,
            handle,
            at,
            notified,
        }
    }

    /// The client retains this notification until its bounded callback drain.
    pub(crate) fn defer(&mut self) {
        *self.notified = Notification::Pending;
    }
}

impl<'scope, H, const DOMAIN: u8, B, D, I> From<Event<'scope, H, DOMAIN, B, D, I>>
    for (
        pin::Pin<&'scope mut H>,
        Slot<'scope, H::Connection, DOMAIN, B, D>,
        id::Handle<I, DOMAIN>,
        Observation<I>,
    )
where
    H: Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn from(value: Event<'scope, H, DOMAIN, B, D, I>) -> Self {
        (value.handler, value.slot, value.handle, value.at)
    }
}

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn established<'scope>(event: Event<'scope, Self, DOMAIN, B, D, I>);
}
