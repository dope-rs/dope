use o3::buffer::{CapacityError, FixedPoolCapacity, Lease, Pooled, RuntimePoolCapacity, Shared};

pub(crate) mod private {
    /// Internal proof that a byte owner cannot be aliased while egress retains it.
    ///
    /// # Safety
    /// `AsRef::as_ref` must keep returning the same immutable allocation until
    /// the owner is dropped.
    pub unsafe trait Sealed {}
}

/// Bytes whose address and contents stay fixed while egress retains them.
///
/// This trait is sealed. Applications select one of the immutable owners
/// provided by `dope-net`; they never need to write an `unsafe impl`.
#[allow(private_bounds)]
pub trait StableBytes: private::Sealed + AsRef<[u8]> {}

impl<T> StableBytes for T where T: private::Sealed + AsRef<[u8]> {}

// SAFETY: Shared retains immutable ref-counted storage.
unsafe impl private::Sealed for Shared {}

// SAFETY: Pooled retains one immutable pool slot.
unsafe impl private::Sealed for Pooled {}

// SAFETY: a lease is uniquely owned. Once moved into egress there is no safe
// path to mutate its allocation until egress returns or drops the owner.
unsafe impl private::Sealed for Lease<'_, RuntimePoolCapacity> {}

// SAFETY: identical to the runtime-capacity lease proof above.
unsafe impl<const CAP: u32> private::Sealed for Lease<'_, FixedPoolCapacity<CAP>> {}

// SAFETY: A shared static slice is immutable for the program lifetime.
unsafe impl private::Sealed for &'static [u8] {}

// SAFETY: A shared static array is immutable for the program lifetime.
unsafe impl<const N: usize> private::Sealed for &'static [u8; N] {}

/// Immutable static bytes carried with arbitrary owned metadata.
///
/// This is useful when completion or Drop behavior belongs to a request while
/// the transmitted payload itself is static. The metadata is never consulted
/// for byte stability.
pub struct StaticBytes<T> {
    bytes: &'static [u8],
    owner: T,
}

impl<T> StaticBytes<T> {
    pub const fn new(bytes: &'static [u8], owner: T) -> Self {
        Self { bytes, owner }
    }

    pub const fn owner(&self) -> &T {
        &self.owner
    }

    pub fn into_owner(self) -> T {
        self.owner
    }
}

impl<T> AsRef<[u8]> for StaticBytes<T> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

// SAFETY: the byte view is a static slice and does not depend on `owner`.
unsafe impl<T> private::Sealed for StaticBytes<T> {}

/// A request builder backed by a uniquely owned pool lease.
///
/// The overflow bit lets protocol encoders remain infallible on their hot path.
/// Moving the completed builder into egress freezes it without allocation,
/// copying, or reference-count traffic.
pub struct LeaseBuffer<'d> {
    lease: Lease<'d>,
    overflowed: bool,
}

impl<'d> LeaseBuffer<'d> {
    pub const fn new(lease: Lease<'d>) -> Self {
        Self {
            lease,
            overflowed: false,
        }
    }

    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn try_push(&mut self, byte: u8) -> Result<(), CapacityError> {
        let result = self.lease.try_push(byte);
        self.overflowed |= result.is_err();
        result
    }

    pub fn try_extend_from_slice(&mut self, src: &[u8]) -> Result<(), CapacityError> {
        let result = self.lease.try_extend_from_slice(src);
        self.overflowed |= result.is_err();
        result
    }

    pub fn len(&self) -> usize {
        self.lease.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lease.is_empty()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.lease.as_mut_slice()
    }

    pub fn into_lease(self) -> Lease<'d> {
        self.lease
    }
}

impl AsRef<[u8]> for LeaseBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        self.lease.as_ref()
    }
}

// SAFETY: LeaseBuffer uniquely owns its lease and exposes mutation only
// through `&mut self`; moving it into egress removes that access from callers.
unsafe impl private::Sealed for LeaseBuffer<'_> {}
