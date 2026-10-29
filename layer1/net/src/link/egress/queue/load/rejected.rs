use o3::mem;

use crate::link::egress::{Load, Rejection, load};

/// Rejected load retaining both its payload and its exact measured charge.
#[must_use]
#[repr(transparent)]
#[derive(Debug)]
pub struct Rejected<L, E = Rejection>(mem::Rejected<L, E>);

impl<L, E> Rejected<L, E> {
    pub(in crate::link) const fn new(load: L, reason: E) -> Self {
        Self(mem::Rejected::new(load, reason))
    }

    pub const fn error(&self) -> &E {
        self.0.error()
    }

    pub const fn value(&self) -> &L {
        self.0.value()
    }

    pub fn into_parts(self) -> (L, E) {
        self.0.into_parts()
    }
}

impl<B> From<Rejected<Load<B>, load::Error>> for Rejected<load::Value<B>> {
    fn from(rejected: Rejected<Load<B>, load::Error>) -> Self {
        let (load, cause) = rejected.into_parts();
        let reason = match cause {
            load::Error::Empty(cause) => Rejection::Empty(cause),
            load::Error::Transfer(cause) => Rejection::Transfer(cause),
            load::Error::Resident(cause) => Rejection::Resident(cause),
        };
        Self::new(load::Value::Unchecked(load), reason)
    }
}
