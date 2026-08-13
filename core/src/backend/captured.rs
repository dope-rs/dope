use std::{marker, mem};

/// Backend value whose erased resources stay tied to one retained owner.
#[must_use]
#[repr(transparent)]
pub(crate) struct Captured<'owner, R> {
    raw: R,
    owner: marker::PhantomData<&'owner mut ()>,
}

impl<'owner, R> Captured<'owner, R> {
    /// Binds an owned backend value to a scope without erasing a Rust borrow.
    pub(in crate::backend) fn scoped(raw: R) -> Self {
        Self {
            raw,
            owner: marker::PhantomData,
        }
    }

    pub(in crate::backend) fn map<T>(self, map: impl FnOnce(R) -> T) -> Captured<'owner, T> {
        Captured::scoped(map(self.raw))
    }

    pub(in crate::backend) fn as_raw(&self) -> &R {
        &self.raw
    }

    pub(in crate::backend) fn into_inner(self) -> R {
        self.raw
    }
}

const _: () = {
    assert!(mem::size_of::<Captured<'static, usize>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Captured<'static, usize>>() == mem::align_of::<usize>());
};
