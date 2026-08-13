use o3::buffer::{self, pool, storage, storage::inline, write};

use crate::link::egress;

/// Immutable bytes retained within one generative driver domain.
pub trait Payload<'d>: egress::raw::Sealed<'d> + AsRef<[u8]> {
    /// Storage kept resident until this payload is released.
    fn resident_bytes(&self) -> usize {
        egress::raw::Sealed::retained_bytes(self)
    }
}

impl<'d, T> Payload<'d> for T where T: egress::raw::Sealed<'d> + AsRef<[u8]> {}

/// Allocation-free sum payload for protocols with distinct data and control
/// frame owners.
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A: AsRef<[u8]>, B: AsRef<[u8]>> AsRef<[u8]> for Either<A, B> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Left(value) => value.as_ref(),
            Self::Right(value) => value.as_ref(),
        }
    }
}

/// Fixed-capacity owned bytes whose address remains stable after moving into
/// an egress queue. This is intended for small protocol control frames that
/// must not allocate on the operation path.
#[repr(transparent)]
pub struct Inline<const N: usize>(inline::WideBytes<N>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Full;

impl<const N: usize> Inline<N> {
    pub const fn new() -> Self {
        Self(inline::WideBytes::new())
    }

    pub fn try_push(&mut self, byte: u8) -> Result<(), Full> {
        self.0.try_push(byte).map_err(|_| Full)
    }

    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), Full> {
        self.0.try_extend(bytes).map_err(|_| Full)
    }

    pub const fn len(&self) -> usize {
        self.0.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<const N: usize> Default for Inline<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> AsRef<[u8]> for Inline<N> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<const N: usize> write::ByteSink for Inline<N> {
    type Error = Full;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        self.try_push(byte)
    }

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.try_extend(bytes)
    }

    fn write_slices<const M: usize>(&mut self, slices: [&[u8]; M]) -> Result<(), Self::Error> {
        self.0.try_extend_from_slices(slices).map_err(|_| Full)
    }
}

const _: () = assert!(std::mem::size_of::<Inline<514>>() == 516);
const _: () =
    assert!(std::mem::size_of::<Inline<514>>() == std::mem::size_of::<inline::WideBytes<514>>());

/// Request builder backed by a uniquely owned pool lease.
///
/// Overflow preserves an infallible encoding hot path until ownership moves to egress.
pub struct Cursor {
    lease: pool::Cursor,
    overflowed: bool,
}

impl Cursor {
    pub const fn new(lease: pool::Cursor) -> Self {
        Self {
            lease,
            overflowed: false,
        }
    }

    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn try_push(&mut self, byte: u8) -> Result<(), buffer::CapacityError> {
        let result = self.lease.try_push(byte);
        self.overflowed |= result.is_err();
        result
    }

    pub fn try_extend(&mut self, src: &[u8]) -> Result<(), buffer::CapacityError> {
        let result = self.lease.try_extend(src);
        self.overflowed |= result.is_err();
        result
    }

    pub fn len(&self) -> usize {
        self.lease.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lease.is_empty()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.lease.len() + self.lease.spare_capacity()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.lease.as_mut_slice()
    }

    pub fn into_lease(self) -> pool::Cursor {
        self.lease
    }
}

impl AsRef<[u8]> for Cursor {
    fn as_ref(&self) -> &[u8] {
        self.lease.as_ref()
    }
}

/// Outbound payload retained for one generative driver domain.
pub enum Buffer<'d> {
    Borrowed(&'d [u8]),
    Shared(storage::Shared),
    Frozen(buffer::Frozen),
}

impl AsRef<[u8]> for Buffer<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Shared(bytes) => bytes.as_ref(),
            Self::Frozen(bytes) => bytes.as_ref(),
        }
    }
}

impl<'d> From<storage::Shared> for Buffer<'d> {
    fn from(bytes: storage::Shared) -> Self {
        Self::Shared(bytes)
    }
}

impl<'d> From<&'d [u8]> for Buffer<'d> {
    fn from(bytes: &'d [u8]) -> Self {
        Self::Borrowed(bytes)
    }
}

impl<'d, const N: usize> From<&'d [u8; N]> for Buffer<'d> {
    fn from(bytes: &'d [u8; N]) -> Self {
        Self::Borrowed(bytes)
    }
}

impl<'d> From<buffer::Frozen> for Buffer<'d> {
    fn from(bytes: buffer::Frozen) -> Self {
        Self::Frozen(bytes)
    }
}
