use o3::mem::{self, lease};

/// Commits the protocol state from which the unchanged payload was taken.
pub trait Commit: mem::Return {
    fn commit(self);
}

/// Keeps a payload inseparable from its original rollback authority.
#[repr(transparent)]
pub struct Lease<R: Commit> {
    inner: mem::Lease<R, lease::Open>,
}

impl<R: Commit> Lease<R> {
    pub(in crate::link::egress) const fn new(owner: R, value: R::Item) -> Self {
        Self {
            inner: mem::Lease::open(owner, value),
        }
    }

    pub(in crate::link::egress) fn into_parts(self) -> (R, R::Item) {
        self.inner.into_parts()
    }
}
