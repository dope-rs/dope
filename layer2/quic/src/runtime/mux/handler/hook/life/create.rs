use o3::buffer;
use std::pin;

use crate::conn::{id, scope};
use crate::runtime::mux::handler;
use crate::stream;

/// Initializes unit connection state without emitting a creation event.
pub struct Unit;

/// Routes connection initialization to a creation sink.
pub struct Call;

/// Application-owned decision made before a transport slot is installed.
pub enum Decision<C> {
    Install(C),
    Reject,
}

pub type Event<'scope, H, const DOMAIN: u8, B, D, I> = crate::runtime::mux::handler::hook::Event<
    (
        pin::Pin<&'scope mut H>,
        scope::Access<'scope, DOMAIN, B, true, D>,
        id::Handle<I, DOMAIN>,
    ),
    3,
>;

pub trait Mode<H, const DOMAIN: u8, B, D, I>
where
    H: handler::Handler<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn create(event: Event<'_, H, DOMAIN, B, D, I>) -> Decision<H::Connection>;
}

impl<H, const DOMAIN: u8, B, D, I> Mode<H, DOMAIN, B, D, I> for Unit
where
    H: handler::Handler<DOMAIN, B, D, I, Connection = ()>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn create(event: Event<'_, H, DOMAIN, B, D, I>) -> Decision<H::Connection> {
        let Event { .. } = event;
        Decision::Install(())
    }
}

impl<H, const DOMAIN: u8, B, D, I> Mode<H, DOMAIN, B, D, I> for Call
where
    H: Sink<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn create(event: Event<'_, H, DOMAIN, B, D, I>) -> Decision<H::Connection> {
        Decision::Install(H::create(event))
    }
}

pub trait Sink<const DOMAIN: u8, B, D, I>:
    handler::Handler<DOMAIN, B, D, I, Create = Call>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn create(event: Event<'_, Self, DOMAIN, B, D, I>) -> Self::Connection;
}
