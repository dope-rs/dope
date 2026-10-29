use std::arch::x86_64::__cpuid;

use crate::Errno;

/// CPUID leaf 1 is mandatory on x86_64; PCLMUL uses its baseline SSE2 state.
pub(in crate::thread) fn try_crc32(_: impl Fn() -> i32) -> Result<bool, Errno> {
    let features = __cpuid(1);
    Ok(features.edx & (1 << 26) != 0 && features.ecx & (1 << 1) != 0)
}
