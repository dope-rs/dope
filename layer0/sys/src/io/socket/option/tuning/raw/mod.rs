mod chain;

use std::{num, pin};

pub use chain::Chain;

use crate::io::socket::option::tuning::{Occupied, device};

/// # Safety
/// Pinned values retain their addresses until the exact chain terminates.
pub unsafe trait State<E: Copy, const N: usize>: Default {
    type Occupied<'a>: Occupied<E>
    where
        Self: 'a;

    fn bind(self: pin::Pin<&mut Self>) -> Option<Self::Occupied<'_>>;

    /// # Safety
    /// No prior chain is live; `count` equals the option CQE count and is at most N.
    /// Keep this owner pinned and all values live through every CQE or quiescence.
    unsafe fn values(
        self: pin::Pin<&mut Self>,
        target: E,
        count: num::NonZero<u32>,
    ) -> &mut [libc::c_int; N];
}

/// Applies options through the host's exact descriptor and native storage policy.
pub trait Apply<B, H: device::raw::Host<B, N>, const N: usize>: device::Owner<H> {
    /// # Safety
    /// Each cursor contains its declared option count; readiness options settle synchronously.
    /// On a ring backend, retain pinned values through terminal completion or queue quiescence.
    unsafe fn try_apply(self) -> Result<H::Output, H::Error>;
}

/// Connects through the backend's exact option and retained-terminal policy.
pub trait Connect<B, H: device::raw::Connect<B, R, N>, R, const N: usize>:
    device::Owner<H>
{
    /// # Safety
    /// The cursor contains exactly its declared option count, at most N; retain terminal owners through completion or quiescence.
    /// Ring backends also retain pinned option values; readiness options settle synchronously.
    unsafe fn try_connect(self) -> Result<H::Output, H::Error>;
}
