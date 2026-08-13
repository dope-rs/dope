use std::ptr;

use crate::io::transfer;

#[repr(transparent)]
struct SharedIovec(libc::iovec);

pub(in crate::backend::uring) struct Header(libc::msghdr);

// SAFETY: both values are immutable process-lifetime input templates. The
// multishot provided-buffer ABI reads the iovec length but writes every result
// into a selected buffer, never through these pointers.
unsafe impl Sync for SharedIovec {}
unsafe impl Sync for Header {}

static DATAGRAM_IOV: SharedIovec = SharedIovec(libc::iovec {
    iov_base: ptr::null_mut(),
    iov_len: transfer::MAX_BYTES,
});

static DATAGRAM: Header = Header(libc::msghdr {
    msg_name: ptr::null_mut(),
    msg_namelen: size_of::<libc::sockaddr_storage>() as libc::socklen_t,
    msg_iov: ptr::from_ref(&DATAGRAM_IOV.0).cast_mut(),
    msg_iovlen: 1,
    msg_control: ptr::null_mut(),
    msg_controllen: 0,
    msg_flags: 0,
});

impl Header {
    pub(in crate::backend::uring) const fn datagram() -> &'static libc::msghdr {
        &DATAGRAM.0
    }
}
