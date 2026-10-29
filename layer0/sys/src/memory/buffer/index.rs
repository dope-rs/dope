use std::num::NonZero;

use o3::base::num::bounded;

/// Exact native buffer identity and its nonzero storage bounds.
pub trait Index: Copy + Eq {
    const ZERO: Self;

    fn advance(&mut self);
    fn offset(self, stride: NonZero<u32>) -> bounded::U64<0, { u64::MAX }>;
    fn is_within(self, capacity: NonZero<u16>) -> bool;
}
