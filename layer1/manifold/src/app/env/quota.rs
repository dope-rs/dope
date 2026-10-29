use std::{mem, num};

/// Per-address connection admission chosen before listener construction.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct PerIp(Option<num::NonZero<u32>>);

impl PerIp {
    pub const UNLIMITED: Self = Self(None);

    pub const fn fixed<const CONNECTIONS: u32>() -> Self {
        const { assert!(CONNECTIONS > 0) };
        Self(Some(
            num::NonZero::<u32>::MIN.saturating_add(CONNECTIONS - 1),
        ))
    }

    pub const fn limit(self) -> Option<num::NonZero<u32>> {
        self.0
    }
}

impl From<num::NonZero<u32>> for PerIp {
    fn from(limit: num::NonZero<u32>) -> Self {
        Self(Some(limit))
    }
}

const _: () = {
    assert!(mem::size_of::<PerIp>() == mem::size_of::<u32>());
    assert!(mem::align_of::<PerIp>() == mem::align_of::<u32>());
};

pub trait Policy {
    const PER_IP: PerIp;
}

pub struct Standard;

impl Policy for Standard {
    const PER_IP: PerIp = PerIp::fixed::<256>();
}
