use std::{cell, marker, mem, net, num, os::fd};

use dope_sys::io::socket::{Prepare as _, gso, msg::Flags};
use gso::{Mode, Source};
use o3::{
    buffer::{
        self,
        bytes::{
            bounded::{Overflow, Value},
            scope,
        },
        resident::Budget,
    },
    mem::Rejected,
};

use crate::{
    Backend,
    driver::result,
    io::{recv, socket, socket::msg, transfer},
};

mod decoded;
mod error;
mod group;
pub mod payload;
pub mod raw;
pub mod source;

pub use decoded::Decoded;
pub use error::Error;
pub use group::Group;

/// Operating-system operation that failed while constructing a routed group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    ReuseAddress,
    ReusePort,
    MapCreate,
    ProgramLoad,
    ProgramAttach,
    Bind,
    MapInsert,
}

impl socket::ReuseStep for Operation {
    const ADDRESS: Self = Self::ReuseAddress;
    const PORT: Self = Self::ReusePort;
}

/// Exact reason a received datagram frame could not produce a packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Frame {
        actual: usize,
        required: usize,
    },
    Name {
        actual: usize,
        capacity: usize,
    },
    Flags {
        actual: u32,
        payload: usize,
        stored: usize,
    },
    Payload {
        actual: usize,
        stored: usize,
    },
}

/// Exact submission, native or frame failure at the datagram receive boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveError {
    Submit(result::SubmitError),
    Socket(crate::Errno),
    Address(socket::Error),
    Rejected(Cause),
    Truncated(Cause),
}

/// Rejected receive storage together with its exact decode cause.
pub struct Failure<'d> {
    buffer: recv::Lease<'d>,
    cause: Cause,
}

impl<'d> Failure<'d> {
    const fn new(buffer: recv::Lease<'d>, cause: Cause) -> Self {
        Self { buffer, cause }
    }

    pub fn into_parts(self) -> (recv::Lease<'d>, Cause) {
        (self.buffer, self.cause)
    }

    /// Returns rejected storage to its original pool and retains the exact cause.
    pub fn release(self) -> Cause {
        self.cause
    }
}

const _: () = assert!(mem::size_of::<Cause>() == 3 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Failure<'static>>() == 6 * mem::size_of::<usize>());

pub(crate) enum Projection<'id> {
    Packet {
        source: net::SocketAddr,
        payload: scope::Range<'id>,
    },
    Address(socket::Error),
    Rejected(Cause),
    Truncated(Cause),
}

impl<'id> Projection<'id> {
    pub(crate) const PREFIX_LEN: usize = 4 * mem::size_of::<u32>();
    pub(crate) const NAME_LEN: usize = socket::Addr::STORAGE_CAPACITY;
    pub(crate) const MIN_LEN: usize = Self::PREFIX_LEN + Self::NAME_LEN;

    pub(crate) fn parse(view: scope::View<'_, 'id>) -> Self {
        let buffer = view.as_slice();
        if buffer.len() < Self::MIN_LEN {
            return Self::Rejected(Cause::Frame {
                actual: buffer.len(),
                required: Self::MIN_LEN,
            });
        }
        let name_len = u32::from_ne_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let data_len = u32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize;
        let flags = Flags::new(u32::from_ne_bytes([
            buffer[12], buffer[13], buffer[14], buffer[15],
        ]));
        let stored = buffer.len() - Self::MIN_LEN;
        if name_len > Self::NAME_LEN {
            return Self::Truncated(Cause::Name {
                actual: name_len,
                capacity: Self::NAME_LEN,
            });
        }
        if flags.is_truncated() {
            return Self::Truncated(Cause::Flags {
                actual: flags.raw(),
                payload: data_len,
                stored,
            });
        }
        let payload = match view.try_get(Self::MIN_LEN..Self::MIN_LEN + data_len) {
            Ok(payload) => payload,
            Err(_) => {
                return Self::Rejected(Cause::Payload {
                    actual: data_len,
                    stored,
                });
            }
        };
        let name = &buffer[Self::PREFIX_LEN..Self::PREFIX_LEN + name_len];
        let source = match socket::Addr::try_parse(name) {
            Ok(source) => source,
            Err(error) => return Self::Address(error),
        };
        Self::Packet { source, payload }
    }
}

#[derive(Clone, Copy)]
pub struct GsoLimits {
    bytes: transfer::Len,
    segments: Segments,
}

/// A nonempty GSO packet count with the minimum required for segmentation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Segments(num::NonZero<u16>);

impl Segments {
    const fn from_count(count: u16) -> Self {
        Self(num::NonZero::<u16>::MIN.saturating_add(count.saturating_sub(2)))
    }

    pub const fn count(self) -> usize {
        self.0.get().saturating_add(1) as usize
    }
}

/// The exact bounded payload and segment size admitted for this backend.
pub struct GsoBatch<P> {
    payload: Value<P>,
    size: num::NonZero<u16>,
}

/// A validated payload paired with the capability for its exact segment shape.
pub struct GsoPayload<P> {
    payload: Value<P>,
    segment: GsoSegment,
}

impl<P> GsoPayload<P> {
    pub fn into_payload(self) -> Value<P> {
        self.payload
    }
}

impl<P: buffer::Residency> buffer::Residency for GsoPayload<P> {
    fn resident(&self) -> usize {
        buffer::Residency::resident(&self.payload)
    }
}

/// Exact reason a GSO segment layout could not be proven valid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    Segments {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    Transfer(Overflow),
    Batch {
        actual: transfer::Len,
        maximum: transfer::Len,
    },
}

impl GsoLimits {
    /// Mechanical array capacity required by the backend's fixed GSO bound.
    pub const fn capacity(self) -> Segments {
        self.segments
    }

    /// Maximum payload bytes admitted by the backend's fixed GSO bound.
    pub const fn max_bytes(self) -> transfer::Len {
        self.bytes
    }

    /// Binds the backend segment shape to the exact bounded payload owner.
    pub fn payload<P: AsRef<[u8]>>(
        self,
        payload: Value<P>,
        size: num::NonZero<u16>,
    ) -> Result<GsoBatch<P>, Rejected<Value<P>, Shape>> {
        let bytes = transfer::Length::len(&payload);
        if bytes.is_greater(self.bytes) {
            return Err(Rejected::new(
                payload,
                Shape::Batch {
                    actual: bytes,
                    maximum: self.bytes,
                },
            ));
        }
        let count = bytes.into_raw().div_ceil(size.get() as usize);
        if count < 2 || count > self.capacity().count() {
            return Err(Rejected::new(
                payload,
                Shape::Segments {
                    actual: count,
                    minimum: 2,
                    maximum: self.capacity().count(),
                },
            ));
        }
        Ok(GsoBatch { payload, size })
    }
}

impl<P> GsoBatch<P> {
    pub fn into_payload(self) -> Value<P> {
        self.payload
    }
}

pub const GSO_LIMITS: Option<GsoLimits> = match <Backend as Source>::LIMITS {
    Some((bytes, segments)) => Some(GsoLimits {
        bytes: transfer::Len::prefix(bytes.into_usize(), transfer::Len::MAX),
        segments: Segments::from_count(segments.get() as u16),
    }),
    None => None,
};

#[repr(transparent)]
pub struct Gso(<<Backend as Source>::Mode as Mode>::Capability);

pub struct GsoSegment {
    size: num::NonZero<u16>,
    gso: Gso,
}

#[repr(transparent)]
pub struct GsoControl(<Backend as Source>::Mode);

impl Gso {
    pub fn acquire() -> Option<Self> {
        <<Backend as Source>::Mode as Mode>::acquire().map(Self)
    }

    pub fn payload<P>(self, batch: GsoBatch<P>) -> GsoPayload<P> {
        let GsoBatch { payload, size } = batch;
        GsoPayload {
            payload,
            segment: GsoSegment { size, gso: self },
        }
    }
}

impl<M: Mode> GsoControl
where
    Backend: Source<Mode = M>,
{
    pub fn new(segment: GsoSegment) -> Self {
        let GsoSegment { size, gso } = segment;
        Self(M::control(gso.0, size))
    }

    pub fn into_segment(self) -> GsoSegment {
        let (gso, size) = M::release(self.0);
        GsoSegment {
            size,
            gso: Gso(gso),
        }
    }
}

impl GsoControl {
    pub fn attach<'a>(&'a mut self, message: &mut msg::Builder<'a>) {
        message.control(Mode::bytes(&mut self.0));
    }
}

const _: () = assert!(mem::size_of::<Gso>() == 0);
const _: () = assert!(mem::align_of::<Gso>() == 1);
const _: () = assert!(mem::size_of::<GsoLimits>() == 2 * mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<Option<GsoSegment>>() == mem::size_of::<u16>());
const _: () = assert!(mem::align_of::<Option<GsoSegment>>() == mem::align_of::<u16>());

/// One affine socket lease from an installed routed group.
#[repr(transparent)]
pub struct Lease {
    socket: socket::Handle,
    thread: marker::PhantomData<cell::Cell<()>>,
}

impl Lease {
    pub fn into_fd(self) -> fd::OwnedFd {
        Backend::into_fd(self.socket.into_inner())
    }

    pub(crate) fn into_handle(self) -> socket::Handle {
        self.socket
    }
}

const _: () = {
    assert!(mem::size_of::<Lease>() == mem::size_of::<fd::OwnedFd>());
    assert!(mem::align_of::<Lease>() == mem::align_of::<fd::OwnedFd>());
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Residency {
    capacity: Capacity,
    resident: Resident,
}

impl From<&Budget<'_>> for Residency {
    fn from(budget: &Budget<'_>) -> Self {
        Self {
            capacity: Capacity(budget.capacity()),
            resident: Resident(budget.resident()),
        }
    }
}

impl Residency {
    pub const fn capacity(self) -> Capacity {
        self.capacity
    }

    pub const fn resident(self) -> Resident {
        self.resident
    }
}

const _: () = assert!(mem::size_of::<Residency>() == 2 * mem::size_of::<usize>());

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Capacity(usize);

impl Capacity {
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn into_raw(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Resident(usize);

impl Resident {
    pub const ZERO: Self = Self(0);

    pub const fn into_raw(self) -> usize {
        self.0
    }
}
