use o3::base::num::bounded;

/// Frame capacity fixed by the receive-message layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Capacity(bounded::U64<0, { u64::MAX }>);

impl From<Capacity> for bounded::U64<0, { u64::MAX }> {
    fn from(value: Capacity) -> Self {
        value.0
    }
}

impl Capacity {
    pub(in crate::reactor::cq::message) const fn from_usize(value: usize) -> Self {
        const { assert!(usize::BITS <= u64::BITS) };
        Self(bounded::U64::clamp(value as u64))
    }
}
