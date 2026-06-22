use std::{io, os::fd::AsRawFd};

use io_uring::IoUring;

pub(super) struct Setup {
    cq_entries: u32,
    ring_entries: u32,
    defer_taskrun: bool,
}

pub(super) struct Built {
    pub uring: IoUring,
    pub defer_active: bool,
}

impl Setup {
    pub(super) fn new(cq_entries: u32, defer_taskrun: bool, ring_entries: u32) -> Self {
        Self { cq_entries, ring_entries, defer_taskrun }
    }

    fn attempt(&self, no_sqarray: bool, single_issuer: bool, defer_taskrun: bool) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();
        builder.setup_submit_all();
        if single_issuer {
            builder.setup_single_issuer();
        }
        if no_sqarray {
            builder.setup_no_sqarray();
        }
        builder.setup_cqsize(self.cq_entries);
        if defer_taskrun {
            builder.setup_defer_taskrun();
        }
        builder.build(self.ring_entries)
    }

    pub(super) fn build(&self) -> io::Result<Built> {
        let fall_back = |e: &io::Error| e.raw_os_error() == Some(libc::EINVAL);
        let mut built = self.attempt(true, true, self.defer_taskrun).map(|u| (u, self.defer_taskrun));
        if built.as_ref().err().is_some_and(fall_back) {
            built = self.attempt(false, true, self.defer_taskrun).map(|u| (u, self.defer_taskrun));
        }
        if built.as_ref().err().is_some_and(fall_back) {
            built = self.attempt(false, false, false).map(|u| (u, false));
        }
        let (uring, defer_active) = built?;

        if !uring.params().is_feature_ext_arg() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "IORING_FEAT_EXT_ARG"));
        }
        if !uring.params().is_feature_nodrop() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "IORING_FEAT_NODROP"));
        }

        Ok(Built { uring, defer_active })
    }

    pub(super) fn register_alloc_range(ring: &IoUring, len: u32) -> io::Result<()> {
        #[repr(C)]
        struct Range {
            off: u32,
            len: u32,
            resv: u64,
        }
        const FILE_ALLOC_RANGE: libc::c_long = 25;
        let range = Range { off: 0, len, resv: 0 };
        // SAFETY: io_uring_register copies one Range struct from `arg`; `range` outlives the call.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                AsRawFd::as_raw_fd(ring) as libc::c_long,
                FILE_ALLOC_RANGE,
                &raw const range as usize as libc::c_long,
                0 as libc::c_long,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}
