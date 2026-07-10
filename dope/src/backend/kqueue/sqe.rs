use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

use crate::backend::socket::{Fd, FdSlot};
use crate::backend::token::{Token, kind};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Timespec(libc::timespec);

impl From<Duration> for Timespec {
    fn from(value: Duration) -> Self {
        Self(libc::timespec {
            tv_sec: value.as_secs() as libc::time_t,
            tv_nsec: value.subsec_nanos() as libc::c_long,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum SqeInner {
    Send {
        slot: FdSlot,
        ptr: *const u8,
        len: u32,
        ud: u64,
    },
    WriteFd {
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
        ud: u64,
    },
    Fsync {
        fd: RawFd,
        ud: u64,
    },
    OpenAt {
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        ud: u64,
    },
    OpenAtFixed {
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        slot: FdSlot,
        ud: u64,
    },
    Read {
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
        ud: u64,
    },
    ReadFixed {
        slot: FdSlot,
        ptr: *mut u8,
        len: u32,
        offset: u64,
        ud: u64,
    },
    Splice {
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        ud: u64,
    },
    SendMsg {
        slot: FdSlot,
        msg: *const libc::msghdr,
        ud: u64,
    },
    AcceptOneshot {
        listener: FdSlot,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        ud: u64,
    },
    RecvMulti {
        slot: FdSlot,
        ud: u64,
    },
    RecvMsgMulti {
        slot: FdSlot,
        msghdr: *const libc::msghdr,
        ud: u64,
    },
    Close {
        slot: FdSlot,
    },
    Quickack,
    Shutdown {
        slot: FdSlot,
        how: i32,
    },
    PollShutdown {
        fd: RawFd,
    },
    Cancel {
        target: u64,
    },
    Interval {
        sec: i64,
        nsec: i64,
        ud: u64,
    },
    SocketAt {
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        ud: u64,
    },
    Connect {
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: u64,
    },
}

#[derive(Clone, Copy)]
pub struct Sqe(pub(super) SqeInner);

impl Sqe {
    pub fn send(fd: &Fd, buf: &[u8], op: Token) -> Self {
        Self::send_at(fd.slot(), buf, op)
    }

    pub(super) fn send_at(slot: FdSlot, buf: &[u8], op: Token) -> Self {
        Self(SqeInner::Send { slot, ptr: buf.as_ptr(), len: buf.len() as u32, ud: op.with_kind(kind::SEND).raw() })
    }

    pub fn write_fd(fd: RawFd, buf: &[u8], offset: u64, op: Token) -> Self {
        Self(SqeInner::WriteFd { fd, ptr: buf.as_ptr(), len: buf.len() as u32, offset, ud: op.with_kind(kind::WRITE).raw() })
    }

    pub fn fsync(fd: RawFd, op: Token) -> Self {
        Self(SqeInner::Fsync { fd, ud: op.with_kind(kind::SYNC).raw() })
    }

    pub fn openat(dir: RawFd, path: *const libc::c_char, flags: i32, mode: u32, op: Token) -> Self {
        Self(SqeInner::OpenAt { dir, path, flags, mode, ud: op.with_kind(kind::OPEN).raw() })
    }

    pub fn openat_fixed(
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        Ok(Self(SqeInner::OpenAtFixed { dir, path, flags, mode, slot, ud: op.with_kind(kind::OPEN).raw() }))
    }

    pub fn read(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Self {
        Self(SqeInner::Read { fd, ptr: buf.as_mut_ptr(), len: buf.len() as u32, offset, ud: op.with_kind(kind::READ).raw() })
    }

    pub fn read_fixed_file(slot: FdSlot, buf: &mut [u8], offset: u64, op: Token) -> Self {
        Self(SqeInner::ReadFixed { slot, ptr: buf.as_mut_ptr(), len: buf.len() as u32, offset, ud: op.with_kind(kind::READ).raw() })
    }

    /// `flags` is accepted for parity with the io_uring backend; the kqueue
    /// bounce emulation has no equivalent and ignores it.
    pub fn splice_raw(
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        _flags: u32,
        op: Token,
    ) -> Self {
        Self(SqeInner::Splice { fd_in, off_in, fd_out, off_out, len, ud: op.with_kind(kind::SPLICE).raw() })
    }

    pub fn recv_multi(fd: &Fd, _buf_group: u16, op: Token) -> Self {
        Self(SqeInner::RecvMulti { slot: fd.slot(), ud: op.with_kind(kind::RECV).raw() })
    }

    pub const SUPPORTS_RECV_DISCARD: bool = false;

    pub fn recv_discard(_fd: &Fd, _remaining: u64, _op: Token) -> Self {
        unreachable!()
    }

    pub fn accept_oneshot(
        listener: &Fd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        op: Token,
    ) -> Self {
        Self(SqeInner::AcceptOneshot { listener: listener.slot(), addr_ptr, addrlen_ptr, ud: op.with_kind(kind::ACCEPT).raw() })
    }

    pub fn recv_msg_multi(
        fd: &Fd,
        msghdr: &libc::msghdr,
        _buf_group: u16,
        op: Token,
    ) -> Self {
        Self(SqeInner::RecvMsgMulti { slot: fd.slot(), msghdr: msghdr as *const _, ud: op.with_kind(kind::RECV).raw() })
    }

    pub fn send_msg(fd: &Fd, msg: &libc::msghdr, op: Token) -> Self {
        Self(SqeInner::SendMsg { slot: fd.slot(), msg: msg as *const _, ud: op.with_kind(kind::SEND).raw() })
    }

    pub fn close(fd: &Fd) -> Self {
        Self(SqeInner::Close { slot: fd.slot() })
    }

    pub fn quickack(_fd: &Fd) -> Self {
        Self(SqeInner::Quickack)
    }

    pub fn shutdown(fd: &Fd, how: i32) -> Self {
        Self(SqeInner::Shutdown { slot: fd.slot(), how })
    }

    pub fn poll_shutdown(fd: RawFd) -> Self {
        Self(SqeInner::PollShutdown { fd })
    }

    pub fn cancel(target: Token, kind: u8) -> Self {
        Self(SqeInner::Cancel { target: target.with_kind(kind).raw() })
    }

    pub fn interval(timer: &Timespec, op: Token) -> Self {
        let ts = timer.0;
        Self(SqeInner::Interval { sec: ts.tv_sec, nsec: ts.tv_nsec, ud: op.with_kind(kind::TIMER).raw() })
    }

    pub fn socket(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        fd: &Fd,
        op: Token,
    ) -> io::Result<Self> {
        Self::socket_at(domain, socket_type, protocol, fd.slot(), op)
    }

    pub(super) fn socket_at(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        Ok(Self(SqeInner::SocketAt { domain, socket_type, protocol, slot, ud: op.with_kind(kind::SOCKET).raw() }))
    }

    pub fn connect(
        fd: &Fd,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        op: Token,
    ) -> Self {
        Self(SqeInner::Connect { slot: fd.slot(), addr_ptr, addr_len, ud: op.with_kind(kind::CONNECT).raw() })
    }
}
