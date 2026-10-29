/// Allocation-free sum payload for protocols with distinct data and control
/// frame owners.
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A: AsRef<[u8]>, B: AsRef<[u8]>> AsRef<[u8]> for Either<A, B> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Left(value) => value.as_ref(),
            Self::Right(value) => value.as_ref(),
        }
    }
}

/// An immutable egress payload with allocation-free application metadata.
/// The tag is retained but not transmitted.
#[repr(C)]
pub struct Tagged<T, K> {
    pub(in crate::link::egress) payload: T,
    tag: K,
}

impl<T, K> Tagged<T, K> {
    pub const fn new(payload: T, tag: K) -> Self {
        Self { payload, tag }
    }

    pub const fn tag(&self) -> &K {
        &self.tag
    }

    pub fn into_parts(self) -> (T, K) {
        (self.payload, self.tag)
    }
}

impl<T: AsRef<[u8]>, K> AsRef<[u8]> for Tagged<T, K> {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}
