use std::marker;

/// A payload whose admission charge satisfies `G`'s resident-owner bound.
#[repr(transparent)]
pub struct Bound<T, G> {
    payload: T,
    proof: marker::PhantomData<fn() -> G>,
}

impl<T, G> Bound<T, G> {
    pub(in crate::link::egress) const fn from_inner(inner: T) -> Self {
        Self {
            payload: inner,
            proof: marker::PhantomData,
        }
    }

    pub fn as_inner(&self) -> &T {
        &self.payload
    }

    pub fn into_inner(self) -> T {
        self.payload
    }
}
