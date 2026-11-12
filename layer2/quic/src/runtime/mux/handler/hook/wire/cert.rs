use std::{net, pin};

use crate::conn::id;

/// An authentication failure borrowing the native error and consumer.
pub type Event<'scope, 'wire, H, const DOMAIN: u8, I> = crate::runtime::mux::handler::hook::Event<
    (
        pin::Pin<&'scope mut H>,
        id::Handle<I, DOMAIN>,
        net::SocketAddr,
        &'wire rustls::Error,
    ),
    0,
>;

pub trait Sink<const DOMAIN: u8, I: id::Temporal>: Sized {
    fn certificate(event: Event<'_, '_, Self, DOMAIN, I>);
}
