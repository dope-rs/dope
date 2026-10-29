use dope::{
    core::driver::route,
    manifold::app::dispatch,
    runtime::{executor, topology::lending},
};

use crate::{Report, Resolver, Stream, net::udp};

// SAFETY: the resolver owns exactly these two pinned native owners, and its
// generated dispatcher includes both shutdown and stopped liveness proofs.
unsafe impl<
    'q,
    'h,
    'p,
    'life,
    's,
    'd: 'life,
    DatagramTag: route::Identity,
    StreamTag: route::Identity,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> lending::raw::Resume<'d>
    for Resolver<'q, 'h, 'p, 'life, 's, 'd, DatagramTag, StreamTag, LANES, ADDRESSES, R>
where
    Self: executor::raw::App<'d>,
    udp::Udp<'life, 's, 'd, DatagramTag, LANES, ADDRESSES, R>: dispatch::raw::Resume<'d>,
    Stream<'q, 'h, 'p, 'life, 's, 'd, StreamTag, LANES, ADDRESSES>: dispatch::raw::Resume<'d>,
{
}
