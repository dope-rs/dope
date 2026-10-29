use std::{marker, mem};

use o3::base::marker::ThreadBound;

/// Zero-sized identity embedded by an installation root.
pub struct Anchor<'a> {
    owner: marker::PhantomData<(&'a mut Anchor<'a>, ThreadBound)>,
}

impl Default for Anchor<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Anchor<'a> {
    /// Creates an unforgeable owner-bound installation identity.
    pub const fn new() -> Self {
        Self {
            owner: marker::PhantomData,
        }
    }

    pub(in crate::driver) fn owner(self) -> marker::PhantomData<(&'a mut Self, ThreadBound)> {
        self.owner
    }
}

/// Exact pinned application authority accepted by driver installation.
/// # Safety
/// `anchor` must consume the implementor's own embedded owner-bound anchor.
pub unsafe trait Root<'a> {
    fn anchor(self) -> Anchor<'a>;
}

const _: () = {
    assert!(mem::size_of::<Anchor<'static>>() == 0);
    assert!(mem::align_of::<Anchor<'static>>() == 1);
    assert!(mem::size_of::<marker::PhantomData<(&mut Anchor<'static>, ThreadBound)>>() == 0);
    assert!(mem::align_of::<marker::PhantomData<(&mut Anchor<'static>, ThreadBound)>>() == 1);
};
