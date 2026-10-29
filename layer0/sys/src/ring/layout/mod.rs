mod region;

use o3::base::num::bounded;
pub use region::Region;

/// Exact kernel-provided queue geometry rejected before mapping access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Layout {
    Entries {
        region: Region,
        actual: bounded::U32<0, { u32::MAX }>,
    },
    Overflow {
        region: Region,
        offset: bounded::U32<0, { u32::MAX }>,
        entries: bounded::U32<0, { u32::MAX }>,
        stride: bounded::U64<0, { u64::MAX }>,
    },
    Offset {
        region: Region,
        offset: bounded::U32<0, { u32::MAX }>,
        bytes: bounded::U64<0, { u64::MAX }>,
        maximum: bounded::U64<0, { u64::MAX }>,
        alignment: bounded::U64<0, { u64::MAX }>,
    },
}
