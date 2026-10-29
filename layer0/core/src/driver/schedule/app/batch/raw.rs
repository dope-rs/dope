use std::num::NonZero;

use o3::mem;

use crate::driver::schedule::app::batch;

/// An immutable, nonzero upper bound for one batch.
///
/// # Safety
/// Every call for the same value must return the same bound until it is consumed.
pub unsafe trait Limit {
    fn get(&self) -> NonZero<usize>;
}

// SAFETY: `NonZero<usize>` is immutable and returns its exact value.
unsafe impl Limit for NonZero<usize> {
    fn get(&self) -> NonZero<usize> {
        *self
    }
}

/// Settles a producer's output under its original admission.
pub trait Settle<'id> {
    /// # Safety
    /// `used` must be the count proved under this exact bound and at most its limit.
    unsafe fn take(self, used: usize) -> mem::raw::Unused<'id>;
}

impl<'id, T: Limit> Settle<'id> for batch::Bound<'id, T> {
    unsafe fn take(self, used: usize) -> mem::raw::Unused<'id> {
        let (limit, token) = self.into_parts();
        // SAFETY: the caller supplies the bound on this exact output; the token
        // retains its original reservation and the first item is implicit.
        unsafe { mem::raw::Unused::new(limit.get().get() - used.max(1), token) }
    }
}
