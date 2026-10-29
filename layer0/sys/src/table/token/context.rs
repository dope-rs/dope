use std::num;

use o3::base::num::bounded;

/// Typed route identities used by native completion controls.
pub trait Context: Copy + Eq {
    type Slot: Copy + Eq + Into<num::NonZero<u32>> + From<bounded::U32<0, 0xff_ffff>>;
    type Control: Copy + Eq + Into<num::NonZero<u64>> + TryFrom<u64, Error = u64>;

    const ZERO: Self::Slot;
    const SHUTDOWN: Self::Control;

    fn control(kind: u8, slot: Self::Slot) -> Self::Control;
}
