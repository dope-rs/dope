use crate::runtime::endpoint::Payload;
use dope::core::driver::route;
pub mod pool;

use std::{marker, mem};

use o3::buffer::resident;

use crate::conn::id;
use crate::runtime::mux::{auth, handler::hook::wire::udp};
use crate::runtime::routing;

/// Connection-local TLS auth owned by an endpoint.
#[derive(Clone, Copy, Default)]
pub struct Owned<R = routing::Plain>(marker::PhantomData<fn() -> R>);

/// External TLS auth borrowed for exactly `'tls`.
#[derive(Clone, Copy, Default)]
pub struct Pooled<'tls, R = routing::Plain> {
    lifetime: marker::PhantomData<&'tls ()>,
    route: marker::PhantomData<fn() -> R>,
}

#[doc(hidden)]
pub trait Binding<'d, const ID: u8> {
    type Mode: auth::Mode<ID, Send = resident::Shared<'d>, Identity = id::Driver<'d>>;
}

impl<'d, const ID: u8, R: routing::LocalRoute> Binding<'d, ID> for Owned<R> {
    type Mode = auth::Owned<R, id::Driver<'d>>;
}

impl<'d, 'tls, R, const ID: u8> Binding<'d, ID> for Pooled<'tls, R>
where
    R: routing::LocalRoute,
{
    type Mode = auth::Pooled<'tls, R, id::Driver<'d>>;
}

const _: () = assert!(mem::size_of::<Pooled<'static>>() == 0);

use crate::proto::{cid, datagram::Size};
use dope::core::driver::schedule::clock;
use dope::manifold::network::datagram;

pub(super) struct Socket<'d, const ID: u8, U: udp::Storage, Q> {
    pub(super) datagram: datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>,
    pub(super) input: U::State<'d, ID>,
    pub(super) rotation: clock::Bound<'d, cid::Schedule>,
    pub(super) packet: Size,
}
