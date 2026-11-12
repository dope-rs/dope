use o3::buffer;
use std::pin;

use crate::conn::id;
use crate::runtime::mux::handler::Handler;
use crate::stream;

pub type Event<'scope, H, const DOMAIN: u8, B, D, I> = crate::runtime::mux::handler::hook::Event<
    (
        pin::Pin<&'scope mut H>,
        &'scope mut <H as Handler<DOMAIN, B, D, I>>::Connection,
        id::Handle<I, DOMAIN>,
    ),
    4,
>;

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn rejected<'scope>(event: Event<'scope, Self, DOMAIN, B, D, I>);
}
