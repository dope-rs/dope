use crate::harness::{peer, peer::scripts};

/// Owns a bounded client-side TCP peer script.
#[must_use = "keep the client alive and call finish to observe its outcome"]
pub struct Client<'scope, T, const N: usize = 1> {
    inner: scripts::Script<'scope, T>,
}

impl<'scope, T, const N: usize> Client<'scope, T, N> {
    pub(super) const fn new(inner: scripts::Script<'scope, T>) -> Self {
        Self { inner }
    }

    pub fn finish(self) -> Result<T, peer::Finish> {
        self.inner.finish()
    }
}
