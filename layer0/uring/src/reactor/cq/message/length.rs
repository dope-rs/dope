use o3::base::num::bounded;

/// Exact frame length observed while decoding a receive-message payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Length(bounded::U64<0, { u64::MAX }>);

impl From<Length> for bounded::U64<0, { u64::MAX }> {
    fn from(value: Length) -> Self {
        value.0
    }
}

impl Length {
    pub(in crate::reactor::cq::message) const fn from_usize(value: usize) -> Self {
        const { assert!(usize::BITS <= u64::BITS) };
        Self(bounded::U64::clamp(value as u64))
    }

    pub(in crate::reactor::cq::message) const fn from_raw(value: u32) -> Self {
        Self(bounded::U64::clamp(value as u64))
    }

    pub(in crate::reactor::cq::message) const fn into_raw(self) -> usize {
        const { assert!(usize::BITS <= u64::BITS) };
        self.0.get() as usize
    }
}
