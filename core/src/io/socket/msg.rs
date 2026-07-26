use core::ptr::addr_of_mut;
use core::ptr::null_mut;
use libc::c_int;
use libc::c_void;
use libc::iovec;
use libc::msghdr;

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct IoVec {
    raw: iovec,
}

impl IoVec {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            raw: iovec {
                iov_base: null_mut(),
                iov_len: 0,
            },
        }
    }

    #[must_use]
    pub fn from_slice(buf: &[u8]) -> Self {
        Self {
            raw: iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: buf.len(),
            },
        }
    }

    #[must_use]
    pub fn from_mut_slice(buf: &mut [u8]) -> Self {
        Self {
            raw: iovec {
                iov_base: buf.as_mut_ptr() as *mut c_void,
                iov_len: buf.len(),
            },
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.raw.iov_len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw.iov_len == 0
    }

    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.raw.iov_base as *const u8
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct MsgHdr {
    raw: msghdr,
}

impl MsgHdr {
    pub fn empty() -> Self {
        Self {
            raw: msghdr {
                msg_name: null_mut(),
                msg_namelen: 0,
                msg_iov: null_mut(),
                msg_iovlen: 0,
                msg_control: null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
        }
    }

    pub fn set_name_ptr(&mut self, ptr: *mut c_void, len: u32) {
        self.raw.msg_name = ptr;
        self.raw.msg_namelen = len;
    }

    pub fn set_namelen(&mut self, len: u32) {
        self.raw.msg_namelen = len;
    }

    pub fn set_iov(&mut self, iov: &[IoVec]) {
        self.raw.msg_iov = iov.as_ptr() as *mut iovec;
        self.raw.msg_iovlen = iov.len() as _;
    }

    pub fn set_control(&mut self, ptr: *mut c_void, len: usize) {
        self.raw.msg_control = ptr;
        self.raw.msg_controllen = len as _;
    }

    pub fn flags(&self) -> c_int {
        self.raw.msg_flags
    }

    pub fn raw(&self) -> &msghdr {
        &self.raw
    }

    pub fn as_mut_ptr(&mut self) -> *mut msghdr {
        addr_of_mut!(self.raw)
    }
}
