use o3::buffer;
use std::pin;

use crate::conn::{id, stream::BoundEvent};
use crate::runtime::mux::{
    self,
    handler::{Handler, Slot},
};
use crate::stream;

type Observation<I> = <I as id::Temporal>::Observation;

pub type Event<'scope, H, const DOMAIN: u8, B, D, I> = crate::runtime::mux::handler::hook::Event<
    (
        pin::Pin<&'scope mut H>,
        Slot<'scope, <H as Handler<DOMAIN, B, D, I>>::Connection, DOMAIN, B, D>,
        id::Handle<I, DOMAIN>,
        BoundEvent<'scope>,
        Observation<I>,
    ),
    6,
>;

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn stream<'scope>(event: Event<'scope, Self, DOMAIN, B, D, I>) -> mux::Disposition;

    fn early<'scope>(event: Event<'scope, Self, DOMAIN, B, D, I>) -> mux::Disposition {
        Self::stream(event)
    }
}
