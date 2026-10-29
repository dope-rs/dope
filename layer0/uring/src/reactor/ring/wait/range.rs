use std::num;

use dope_sys::{io::reactor::submit::Target as _, probe::accept};
use o3::base::num::bounded;

use crate::reactor::{cq, sq};

/// Created only after the ring admits this direct-accept allocation range.
#[derive(Debug)]
#[repr(transparent)]
pub struct Range(num::NonZero<u32>);

impl Range {
    pub(crate) const fn new(position: bounded::U32<1, { 1 << 24 }>) -> Self {
        Self(num::NonZero::<u32>::MIN.saturating_add(position.get() - 1))
    }
}

impl accept::Range for Range {
    type Target = sq::Fixed;
    type Value = cq::Value;

    fn fixed(&self) -> Self::Target {
        let bytes = (self.0.get() - 1).to_le_bytes();
        sq::Fixed::from_le([bytes[0], bytes[1], bytes[2]])
    }

    fn accepted(&self, observed: Self::Value) -> Result<Self::Target, accept::Outside> {
        let value: bounded::U32<0, { i32::MAX as u32 }> = observed.into();
        if value.get() >= 1 << 24 {
            return Err(accept::Outside::Abi(value));
        }
        let limit = self.0.get() - 1;
        if value.get() >= limit {
            return Err(accept::Outside::Capacity {
                actual: bounded::U32::clamp(value.get() as usize),
                limit: bounded::U32::clamp(limit as usize),
            });
        }
        let bytes = value.get().to_le_bytes();
        Ok(sq::Fixed::from_le([bytes[0], bytes[1], bytes[2]]))
    }
}

const _: () = {
    assert!(size_of::<Range>() == size_of::<u32>());
    assert!(size_of::<Option<Range>>() == size_of::<u32>());
};
