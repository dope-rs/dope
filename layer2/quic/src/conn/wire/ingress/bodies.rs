use dope::manifold::network::datagram::packet as datagram;
use o3::buffer::{self, storage::provided};

use std::{mem, ops, time};

use crate::{conn, security::crypto, stream, wire::packet};
use conn::wire::ingress;
use frames::sources;
use ingress::{events, frames};
use packet::{cid, kind, long};
use stream::retain;

pub(in crate::conn::wire::ingress) trait LongKind:
    kind::Long
{
    const FRAME_KIND: frames::PacketKind;
}

impl LongKind for kind::Initial {
    const FRAME_KIND: frames::PacketKind = frames::PacketKind::INITIAL;
}

impl LongKind for kind::Handshake {
    const FRAME_KIND: frames::PacketKind = frames::PacketKind::HANDSHAKE;
}

impl LongKind for kind::ZeroRtt {
    const FRAME_KIND: frames::PacketKind = frames::PacketKind::ZERO_RTT;
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(in crate::conn::wire::ingress) enum Cid {
    None,
    Short(cid::Len, time::Duration),
}

/// Authenticated body owning the packet storage from which it was derived.
#[must_use = "an authenticated packet body must be committed or deliberately dropped"]
pub(crate) struct Packet<P: buffer::raw::Source> {
    packet: P,
    meta: frames::PacketMeta,
    body: ops::Range<usize>,
    cid: Cid,
}

impl<P: long::Buffer> Packet<P> {
    pub(in crate::conn::wire::ingress) fn long<K: LongKind>(
        plain: long::Decrypted<K, P>,
        now: time::Instant,
    ) -> Self {
        let (pn, packet, body) = plain.into_bound();
        Self {
            packet,
            meta: frames::PacketMeta::new(K::FRAME_KIND, pn, now),
            body,
            cid: Cid::None,
        }
    }

    pub(in crate::conn::wire::ingress) fn one_rtt(
        opened: crypto::Opened<P>,
        now: time::Instant,
        pto: time::Duration,
    ) -> Self {
        let width = opened.cid_len();
        let (pn, packet, body) = opened.into_parts();
        Self {
            packet,
            meta: frames::PacketMeta::new(frames::PacketKind::ONE_RTT, pn, now),
            body,
            cid: Cid::Short(width, pto),
        }
    }
}

impl<P: buffer::raw::Source> Packet<P> {
    fn freeze(self) -> Packet<P::Output>
    where
        P: buffer::raw::Freeze,
    {
        Packet {
            packet: self.packet.freeze(),
            meta: self.meta,
            body: self.body,
            cid: self.cid,
        }
    }

    fn ingest<const DOMAIN: u8, B: stream::Recv, const RECONCILE: bool, D: buffer::raw::Segment>(
        &self,
        input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
        source: impl sources::Source<B>,
        hooks: &mut impl events::Sink,
    ) -> Result<(), conn::Error> {
        let packet = self.packet.as_ref();
        let cid = match self.cid {
            Cid::None => None,
            Cid::Short(width, pto) => Some(frames::PacketCid {
                routed: input.context.route(),
                // SAFETY: one_rtt retains the short header and its admitted width together.
                bytes: unsafe { packet.get_unchecked(1..width.end()) },
                pto,
            }),
        };
        // SAFETY: the constructor transfers the authenticated range and its original
        // storage together. Freeze preserves its exact backing, extent and contents.
        let body = unsafe { packet.get_unchecked(self.body.clone()) };
        frames::ingest::try_process(input, self.meta, cid, body, source, hooks)
    }
}

/// Static authenticated-owner projection into the frame source used by commit.
pub(crate) trait Policy<P: long::Buffer, B: stream::Recv>: Copy {
    fn try_process<const DOMAIN: u8, const RECONCILE: bool, D: buffer::raw::Segment>(
        self,
        input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
        packet: Packet<P>,
        hooks: &mut impl events::Sink,
    ) -> Result<(), conn::Error>;
}

impl<'a, B: stream::Recv> Policy<&'a mut [u8], B> for () {
    fn try_process<const DOMAIN: u8, const RECONCILE: bool, D: buffer::raw::Segment>(
        self,
        input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
        auth: Packet<&'a mut [u8]>,
        hooks: &mut impl events::Sink,
    ) -> Result<(), conn::Error> {
        auth.ingest(input, sources::Copied::<B>::new(), hooks)
    }
}

impl<'d, const ADAPTIVE: bool, P: long::Buffer + buffer::raw::Freeze>
    Policy<P, stream::RecvBuffer<'d, ADAPTIVE>> for datagram::Retainer<'d>
where
    P::Output: datagram::Backing<Storage: provided::Frozen<'d, Account: retain::Retain<'d>>>,
{
    fn try_process<const DOMAIN: u8, const RECONCILE: bool, D: buffer::raw::Segment>(
        self,
        input: &mut ingress::Input<'_, DOMAIN, stream::RecvBuffer<'d, ADAPTIVE>, RECONCILE, D>,
        auth: Packet<P>,
        hooks: &mut impl events::Sink,
    ) -> Result<(), conn::Error> {
        let auth = auth.freeze();
        let source = sources::Retained::new(&auth.packet, self, auth.body.start);
        auth.ingest(input, source, hooks)
    }
}

const _: () = {
    assert!(mem::size_of::<Cid>() <= 24);
    assert!(!mem::needs_drop::<Packet<&'static mut [u8]>>());
};
