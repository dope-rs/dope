use o3::buffer;
use std::pin;

use crate::conn::id;
use crate::runtime::mux::handler::{Handler, net};
use crate::stream;

pub type Event<'scope, H, const DOMAIN: u8, I> = crate::runtime::mux::handler::hook::Event<
    (pin::Pin<&'scope mut H>, net::Event<'scope, I, DOMAIN>),
    2,
>;

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn socket<'scope>(event: Event<'scope, Self, DOMAIN, I>);
}
