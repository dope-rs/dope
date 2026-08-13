use std::{mem, net, num};

use crate::{
    backend,
    io::{
        recv,
        socket::{self, msg},
        transfer,
    },
    platform,
};

type Gso = <backend::Backend as platform::Datagram>::Gso;
type GsoCapabilityState = <Gso as platform::GsoMode>::Capability;
type GsoControlState = <Gso as platform::GsoMode>::Control;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GsoLimits {
    pub max_bytes: usize,
    pub max_segments: usize,
}

/// Slot length that guarantees at least one datagram payload byte on every backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct SlotLen(num::NonZeroU32);

impl SlotLen {
    /// Common ceiling: io_uring UAPI fields plus backend socket-address storage.
    const MAX_BACKEND_PREFIX: u32 =
        (4 * mem::size_of::<u32>() + socket::Addr::STORAGE_CAPACITY) as u32;

    pub(crate) const MIN_BYTES: u32 = Self::MAX_BACKEND_PREFIX + 1;

    pub(crate) const fn for_payload(bytes: u32) -> Option<Self> {
        if bytes == 0 {
            return None;
        }
        let Some(slot_bytes) = Self::MAX_BACKEND_PREFIX.checked_add(bytes) else {
            return None;
        };
        if slot_bytes > transfer::MAX_BYTES as u32 {
            return None;
        }
        let Some(slot_bytes) = num::NonZeroU32::new(slot_bytes) else {
            return None;
        };
        Some(Self(slot_bytes))
    }

    pub(crate) const fn nonzero(self) -> num::NonZeroU32 {
        self.0
    }
}

const _: () = assert!(mem::size_of::<SlotLen>() == mem::size_of::<u32>());

pub(crate) enum Projection {
    Packet {
        source: net::SocketAddr,
        payload: recv::Span,
    },
    Rejected {
        truncated: bool,
    },
}

pub const GSO_LIMITS: Option<GsoLimits> = <Gso as platform::GsoMode>::LIMITS;

#[repr(transparent)]
pub struct GsoCapability(GsoCapabilityState);

pub struct GsoSegment {
    segment_size: num::NonZeroU16,
    capability: GsoCapability,
}

#[repr(transparent)]
pub struct GsoControl(GsoControlState);

const _: () = assert!(mem::size_of::<GsoCapability>() == 0);
const _: () = assert!(mem::align_of::<GsoCapability>() == 1);
const _: () = assert!(mem::size_of::<Option<GsoSegment>>() <= mem::size_of::<u16>());
const _: () = assert!(mem::align_of::<Option<GsoSegment>>() <= mem::align_of::<u16>());

impl GsoCapability {
    pub fn acquire() -> Option<Self> {
        <Gso as platform::GsoMode>::acquire().map(Self)
    }

    pub fn limits(&self) -> GsoLimits {
        <Gso as platform::GsoMode>::limits(&self.0)
    }

    pub fn segment(self, segment_size: num::NonZeroU16) -> GsoSegment {
        GsoSegment {
            segment_size,
            capability: self,
        }
    }
}

impl GsoControl {
    pub fn new(segment: GsoSegment) -> Self {
        let GsoSegment {
            segment_size,
            capability,
        } = segment;
        Self(<Gso as platform::GsoMode>::control(
            capability.0,
            segment_size,
        ))
    }

    pub fn attach<'a>(&'a mut self, message: &mut msg::Builder<'a>) {
        <Gso as platform::GsoMode>::attach(&mut self.0, message);
    }

    pub fn into_segment(self) -> GsoSegment {
        let (capability, segment_size) = <Gso as platform::GsoMode>::release(self.0);
        GsoSegment {
            segment_size,
            capability: GsoCapability(capability),
        }
    }
}

pub enum Decoded<'d> {
    Packet {
        source: net::SocketAddr,
        payload: recv::View<'d>,
    },
    Malformed(recv::Lease<'d>),
    Truncated(recv::Lease<'d>),
}

impl<'d> Decoded<'d> {
    pub fn decode(buffer: recv::Lease<'d>) -> Self {
        match <backend::Backend as platform::Datagram>::project(&buffer) {
            Projection::Packet { source, payload } => match buffer.into_subview(payload) {
                Ok(payload) => Self::Packet { source, payload },
                Err(buffer) => Self::Malformed(buffer),
            },
            Projection::Rejected { truncated: false } => Self::Malformed(buffer),
            Projection::Rejected { truncated: true } => Self::Truncated(buffer),
        }
    }
}
