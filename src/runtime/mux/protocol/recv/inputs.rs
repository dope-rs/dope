use crate::conn::wire::ingress::{self, events};
use crate::stream::retain;
use crate::wire::packet::long;
use o3::buffer::{self, storage::provided};
use std::time;

use crate::conn::{scope, wire::path};
use crate::runtime::brand;
use crate::{conn, runtime::mux, stream};
use dope::manifold::network::datagram::packet;
use mux::auth;

pub(in crate::runtime) trait Input<const DOMAIN: u8, B, A>:
    buffer::raw::Mutable + buffer::raw::Store
where
    B: stream::Recv,
    A: auth::Mode<DOMAIN>,
{
    type Retain: Copy;

    fn receive<'access, E>(
        packet: long::Datagram<Self>,
        keep: Self::Retain,
        scope: scope::Access<'access, DOMAIN, B, true, A::Send>,
        work: &'access mut conn::Workspace,
        route: brand::Branded<'access, Option<path::LocalCidKey>>,
        now: time::Instant,
        sink: &'access mut E,
    ) -> conn::Ingress<ingress::Received>
    where
        B: 'access,
        A::Send: 'access,
        E: events::Sink + 'access;
}

impl<const DOMAIN: u8, B, A> Input<DOMAIN, B, A> for &mut [u8]
where
    B: stream::Recv,
    A: auth::Mode<DOMAIN>,
{
    type Retain = ();

    fn receive<'access, E>(
        packet: long::Datagram<Self>,
        _: Self::Retain,
        mut scope: scope::Access<'access, DOMAIN, B, true, A::Send>,
        work: &'access mut conn::Workspace,
        route: brand::Branded<'access, Option<path::LocalCidKey>>,
        now: time::Instant,
        sink: &'access mut E,
    ) -> conn::Ingress<ingress::Received>
    where
        B: 'access,
        A::Send: 'access,
        E: events::Sink + 'access,
    {
        let mut input = scope.input(work).with_route(*route.value());
        let out = input.receive_with(packet, now, sink);
        input.finish(out)
    }
}

impl<
    'turn,
    'd,
    const DOMAIN: u8,
    const ADAPTIVE: bool,
    A,
    T: provided::Mutable<'d, Account: retain::Retain<'d>> + buffer::raw::Store,
> Input<DOMAIN, stream::RecvBuffer<'d, ADAPTIVE>, A> for packet::Packet<'turn, 'd, T>
where
    A: auth::Mode<DOMAIN>,
{
    type Retain = packet::Retainer<'d>;

    fn receive<'access, E>(
        packet: long::Datagram<Self>,
        keep: Self::Retain,
        mut scope: scope::Access<'access, DOMAIN, stream::RecvBuffer<'d, ADAPTIVE>, true, A::Send>,
        work: &'access mut conn::Workspace,
        route: brand::Branded<'access, Option<path::LocalCidKey>>,
        now: time::Instant,
        sink: &'access mut E,
    ) -> conn::Ingress<ingress::Received>
    where
        'd: 'access,
        A::Send: 'access,
        E: events::Sink + 'access,
    {
        let mut input = scope.input(work).with_route(*route.value());
        let out = input.recv(packet, keep, now, sink);
        input.finish(out)
    }
}
