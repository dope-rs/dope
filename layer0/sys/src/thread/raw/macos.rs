use std::{mem, ptr};

use crate::{Errno, kernel};

pub(in crate::thread) fn try_crc32(errno: impl Fn() -> i32) -> Result<bool, Errno> {
    let mut failure = None;
    for name in [c"hw.optional.armv8_crc32", c"hw.optional.arm.FEAT_CRC32"] {
        let mut enabled = 0_i32;
        let mut len = mem::size_of_val(&enabled);
        // SAFETY: both keys return a native integer into exclusive
        // initialized storage; the read-only query receives its extent.
        let result = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                (&mut enabled as *mut i32).cast(),
                &mut len,
                ptr::null_mut(),
                0,
            )
        };
        // SAFETY: Thread supplies Source::read; a failed sysctlbyname leaves
        // its positive native errno unchanged until this immediate leaf decode.
        match unsafe {
            kernel::raw::Status::new(result)
                .try_zero::<Errno>(|| kernel::raw::Code::new(errno()).into())
        } {
            Ok(()) if len == mem::size_of_val(&enabled) && enabled != 0 => return Ok(true),
            Ok(()) => {}
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    failure.map_or(Ok(false), Err)
}
