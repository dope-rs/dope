use crate::proto::path::Peer;
use std::num;

use crate::wire::packet::long::retry;
use dope::core::io::datagram::GsoPayload;
use o3::base::marker::ThreadBound;
use o3::buffer::{self, storage};

use crate::runtime::mux::output;

/// One nonempty allocation crossing the mux-to-proto ownership boundary.
#[repr(transparent)]
pub struct Flight(Kind, ThreadBound);

pub(in crate::runtime) enum Kind {
    Plain(Peer, storage::NonEmpty<Vec<u8>>),
    Suffix(Peer, retry::Suffix),
    Batch(
        Peer,
        storage::NonEmpty<Vec<u8>>,
        num::NonZero<u16>,
        num::NonZero<u8>,
    ),
    Segmented(Peer, GsoPayload<Vec<u8>>),
}

impl Flight {
    pub(in crate::runtime) fn plain(addr: Peer, payload: storage::NonEmpty<Vec<u8>>) -> Self {
        Self(Kind::Plain(addr, payload), ThreadBound::NEW)
    }

    pub(in crate::runtime) fn suffix(addr: Peer, payload: retry::Suffix) -> Self {
        Self(Kind::Suffix(addr, payload), ThreadBound::NEW)
    }

    pub(in crate::runtime) fn batch(
        addr: Peer,
        payload: storage::NonEmpty<Vec<u8>>,
        size: num::NonZero<u16>,
        packets: num::NonZero<u8>,
    ) -> Self {
        Self(Kind::Batch(addr, payload, size, packets), ThreadBound::NEW)
    }

    pub(in crate::runtime) fn segmented(addr: Peer, payload: GsoPayload<Vec<u8>>) -> Self {
        Self(Kind::Segmented(addr, payload), ThreadBound::NEW)
    }

    pub(in crate::runtime) fn into_kind(self) -> Kind {
        self.0
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        match self.0 {
            Kind::Plain(addr, _)
            | Kind::Suffix(addr, _)
            | Kind::Batch(addr, ..)
            | Kind::Segmented(addr, _) => addr.addr(),
        }
    }

    pub fn payload(&self) -> &[u8] {
        match &self.0 {
            Kind::Plain(_, payload) | Kind::Batch(_, payload, ..) => payload,
            Kind::Suffix(_, payload) => payload.as_ref(),
            Kind::Segmented(_, payload) => payload.as_ref(),
        }
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        match &mut self.0 {
            Kind::Plain(_, payload) | Kind::Batch(_, payload, ..) => payload,
            Kind::Suffix(_, payload) => payload.as_mut(),
            Kind::Segmented(_, payload) => payload.as_mut(),
        }
    }

    /// Iterates over the exact UDP datagrams carried by this allocation.
    pub fn segments_mut(&mut self) -> output::Segments<'_> {
        match &mut self.0 {
            Kind::Plain(_, payload) => output::Segments::one(payload),
            Kind::Suffix(_, payload) => output::Segments::one(payload.as_mut()),
            Kind::Batch(_, payload, size, _) => output::Segments::batch(payload, (*size).into()),
            Kind::Segmented(_, payload) => {
                let size = payload.segment_size();
                output::Segments::batch(payload.as_mut(), size.into())
            }
        }
    }

    pub fn packet_count(&self) -> num::NonZero<usize> {
        match &self.0 {
            Kind::Plain(_, _) | Kind::Suffix(_, _) => num::NonZero::<usize>::MIN,
            Kind::Batch(_, _, _, packets) => (*packets).into(),
            Kind::Segmented(_, payload) => payload.packet_count().into(),
        }
    }

    pub fn byte_len(&self) -> usize {
        self.payload().len()
    }

    pub const fn is_segmented(&self) -> bool {
        matches!(self.0, Kind::Batch(..) | Kind::Segmented(..))
    }

    pub const fn is_suffix(&self) -> bool {
        matches!(self.0, Kind::Suffix(..))
    }

    pub(in crate::runtime) const fn segment_size(&self) -> Option<num::NonZero<u16>> {
        match &self.0 {
            Kind::Batch(_, _, size, _) => Some(*size),
            Kind::Segmented(_, payload) => Some(payload.segment_size()),
            Kind::Plain(..) | Kind::Suffix(..) => None,
        }
    }

    pub(in crate::runtime::mux) fn into_storage(self) -> Vec<u8> {
        match self.0 {
            Kind::Plain(_, payload) | Kind::Batch(_, payload, ..) => payload.into_storage(),
            Kind::Suffix(_, payload) => payload.into_storage(),
            Kind::Segmented(_, payload) => payload.into_payload().into_inner(),
        }
    }
}

impl buffer::Residency for Flight {
    fn resident(&self) -> usize {
        match &self.0 {
            Kind::Plain(_, payload) | Kind::Batch(_, payload, ..) => payload.resident(),
            Kind::Suffix(_, payload) => payload.resident(),
            Kind::Segmented(_, payload) => payload.resident(),
        }
    }
}

// The original 64-bit Flight is 104 bytes, including its enum tag.
const _: () = assert!(size_of::<Flight>() == 104);
