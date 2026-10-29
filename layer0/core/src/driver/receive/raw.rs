use std::num;

use o3::depot;

use crate::driver::{receive::Index, schedule::ready};

// SAFETY: receive indices are the complete zero-based u16 ready domain;
// scalar conversions are deterministic and cannot panic or reenter storage.
unsafe impl depot::raw::Compact for Index {
    const MAX: u32 = u16::MAX as u32 - 1;

    fn into_raw(self) -> u32 {
        self.0.raw()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        let code = unsafe { num::NonZero::<u16>::new_unchecked(raw as u16 + 1) };
        Self(ready::DynamicIndex::from_code(code))
    }
}
