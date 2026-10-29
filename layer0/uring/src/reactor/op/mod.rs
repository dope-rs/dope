use std::ptr;

pub(crate) use code::Code;
use libc::socklen_t;

pub(crate) mod buffers;
mod code;
pub(crate) mod controls;
pub(crate) mod files;
pub(crate) mod messages;
pub(crate) mod sockets;

/// Immutable multishot receive template, promoted for every asynchronous kernel read.
pub(crate) const RECEIVE_HEADER: &libc::msghdr = &libc::msghdr {
    msg_name: ptr::null_mut(),
    msg_namelen: size_of::<libc::sockaddr_storage>() as socklen_t,
    msg_iov: ptr::null_mut(),
    msg_iovlen: 0,
    msg_control: ptr::null_mut(),
    msg_controllen: 0,
    msg_flags: 0,
};

const _: () = {
    assert!(size_of::<libc::sockaddr_storage>() <= socklen_t::MAX as usize);
};
