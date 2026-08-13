use std::io;

pub(in crate::backend::uring) struct Fixed(u32);

impl Fixed {
    pub(in crate::backend::uring) const fn new(len: u32) -> Self {
        Self(len)
    }

    pub(in crate::backend::uring) fn register(self, ring: &io_uring::IoUring) -> io::Result<()> {
        use libc::c_long;

        #[repr(C)]
        struct Range {
            off: u32,
            len: u32,
            resv: u64,
        }

        const FILE_ALLOC_RANGE: c_long = 25;
        let range = Range {
            off: 0,
            len: self.0,
            resv: 0,
        };
        // SAFETY: ring is live and Range is the register opcode's exact ABI payload.
        let result = unsafe {
            use std::os::fd::AsRawFd;

            use libc::{SYS_io_uring_register, syscall};
            syscall(
                SYS_io_uring_register,
                AsRawFd::as_raw_fd(ring) as c_long,
                FILE_ALLOC_RANGE,
                &raw const range as usize as c_long,
                0 as c_long,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}
