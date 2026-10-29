//! Statically dispatched connector ingress storage policies.

use std::num;

mod buffer;
mod buffered;
mod direct;
mod empty;
mod policy;
mod seal;

pub(crate) use buffer::Buffer;
pub use buffered::Buffered;
pub use direct::Direct;
pub(crate) use empty::Empty;
pub use policy::Policy;
pub(crate) use seal::Seal;

/// Exact parser staging bound selected by its validated policy.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Limit(pub(super) Bound);

#[derive(Clone, Copy)]
pub(super) enum Bound {
    Direct,
    Buffered(num::NonZero<u32>),
}

const _: () = assert!(size_of::<Limit>() == size_of::<num::NonZero<u32>>());
