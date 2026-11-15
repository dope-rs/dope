use std::{net::SocketAddr, ops::ControlFlow, pin::Pin};

use o3::{alloc, buffer, depot::capacity};

use crate::{conn::id, runtime::endpoint::reactor::input, runtime::mux::handler, stream};

pub struct Off;
pub struct On<const N: u32>;

pub trait Sink<const ID: u8, B, D, I>: handler::Handler<ID, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    /// Passes the original datagram to QUIC, or consumes it with an optional reply to its source.
    fn udp(self: Pin<&mut Self>, source: SocketAddr, packet: &[u8])
    -> ControlFlow<Option<Vec<u8>>>;
}

pub trait Storage {
    type State<'d, const ID: u8>;

    fn create<'d, const ID: u8>() -> Result<Self::State<'d, ID>, alloc::Error>;
    fn state<'a, 'd, const ID: u8>(
        state: &'a mut Self::State<'d, ID>,
    ) -> Option<&'a mut input::State<'d, ID>>;
    fn pending<'d, const ID: u8>(state: &Self::State<'d, ID>) -> bool;
}

impl Storage for Off {
    type State<'d, const ID: u8> = ();

    fn create<'d, const ID: u8>() -> Result<Self::State<'d, ID>, alloc::Error> {
        Ok(())
    }
    fn state<'a, 'd, const ID: u8>(_: &'a mut ()) -> Option<&'a mut input::State<'d, ID>> {
        None
    }
    fn pending<'d, const ID: u8>(_: &()) -> bool {
        false
    }
}

impl<const N: u32> Storage for On<N> {
    type State<'d, const ID: u8> = input::State<'d, ID>;

    fn create<'d, const ID: u8>() -> Result<Self::State<'d, ID>, alloc::Error> {
        input::State::try_new(capacity::Count::new(N))
    }
    fn state<'a, 'd, const ID: u8>(
        state: &'a mut Self::State<'d, ID>,
    ) -> Option<&'a mut input::State<'d, ID>> {
        Some(state)
    }
    fn pending<'d, const ID: u8>(state: &Self::State<'d, ID>) -> bool {
        state.runnable()
    }
}

pub trait Mode<H, const ID: u8, B, D, I>: Storage
where
    H: handler::Handler<ID, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn call(
        handler: Pin<&mut H>,
        source: SocketAddr,
        packet: &[u8],
    ) -> ControlFlow<Option<Vec<u8>>>;
}

impl<H, const ID: u8, B, D, I> Mode<H, ID, B, D, I> for Off
where
    H: handler::Handler<ID, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn call(_: Pin<&mut H>, _: SocketAddr, _: &[u8]) -> ControlFlow<Option<Vec<u8>>> {
        ControlFlow::Continue(())
    }
}

impl<H, const ID: u8, B, D, I, const N: u32> Mode<H, ID, B, D, I> for On<N>
where
    H: Sink<ID, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn call(
        handler: Pin<&mut H>,
        source: SocketAddr,
        packet: &[u8],
    ) -> ControlFlow<Option<Vec<u8>>> {
        H::udp(handler, source, packet)
    }
}
