use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::slice;
use std::time::Duration;

use o3::marker::ThreadBound;

use crate::driver::token::{Token, kind};
use crate::io::fd::{Fd, FdSlot};

#[derive(Clone, Copy)]
pub struct TimerSpec {
    sec: i64,
    nsec: i64,
}

impl From<Duration> for TimerSpec {
    fn from(value: Duration) -> Self {
        Self {
            sec: value.as_secs().min(i64::MAX as u64) as i64,
            nsec: i64::from(value.subsec_nanos()),
        }
    }
}

pub enum SqeInner {
    Send {
        slot: FdSlot,
        ptr: *const u8,
        len: u32,
        ud: Token,
    },
    WriteFd {
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
        ud: Token,
    },
    OpenAt {
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        ud: Token,
    },
    OpenAtFixed {
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        slot: FdSlot,
        ud: Token,
    },
    Read {
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
        ud: Token,
    },
    ReadFixed {
        slot: FdSlot,
        ptr: *mut u8,
        len: u32,
        offset: u64,
        ud: Token,
    },
    StatPath {
        path: *const libc::c_char,
        stat: *mut libc::stat,
        ud: Token,
    },
    StatFd {
        fd: RawFd,
        stat: *mut libc::stat,
        ud: Token,
    },
    Splice {
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        ud: Token,
    },
    SendMsg {
        slot: FdSlot,
        msg: *const libc::msghdr,
        ud: Token,
    },
    AcceptOneshot {
        listener: FdSlot,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        ud: Token,
    },
    RecvMulti {
        slot: FdSlot,
        ud: Token,
    },
    RecvMsgMulti {
        slot: FdSlot,
        msghdr: *const libc::msghdr,
        ud: Token,
    },
    Quickack,
    Shutdown {
        slot: FdSlot,
        how: i32,
    },
    Cancel {
        target: Token,
    },
    Interval {
        sec: i64,
        nsec: i64,
        ud: Token,
    },
    CancelCreate {
        slot: FdSlot,
    },
    SocketAt {
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        ud: Token,
    },
    Connect {
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: Token,
    },
}

pub struct Sqe(pub SqeInner, ThreadBound);

impl Sqe {
    fn new(inner: SqeInner) -> Self {
        Self(inner, ThreadBound::NEW)
    }

    pub fn send(fd: &Fd, buf: &[u8], op: Token) -> Self {
        Self::send_at(fd.slot(), buf, op)
    }

    pub fn send_at(slot: FdSlot, buf: &[u8], op: Token) -> Self {
        Self::new(SqeInner::Send {
            slot,
            ptr: buf.as_ptr(),
            len: buf.len() as u32,
            ud: op.with_kind(kind::SEND),
        })
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unchanged until completion.
    pub unsafe fn write_fd(fd: RawFd, buf: &[u8], offset: u64, op: Token) -> Self {
        Self::new(SqeInner::WriteFd {
            fd,
            ptr: buf.as_ptr(),
            len: buf.len() as u32,
            offset,
            ud: op.with_kind(kind::WRITE),
        })
    }

    pub fn openat(dir: RawFd, path: *const libc::c_char, flags: i32, mode: u32, op: Token) -> Self {
        Self::new(SqeInner::OpenAt {
            dir,
            path,
            flags,
            mode,
            ud: op.with_kind(kind::OPEN),
        })
    }

    /// # Safety
    /// `dir` and a NUL-terminated `path` must stay valid, and `slot` reserved, until completion.
    pub unsafe fn openat_fixed(
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        Ok(Self::new(SqeInner::OpenAtFixed {
            dir,
            path,
            flags,
            mode,
            slot,
            ud: op.with_kind(kind::OPEN),
        }))
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unaliased until completion.
    pub unsafe fn read(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Self {
        let buf = unsafe {
            slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<MaybeUninit<u8>>(), buf.len())
        };
        unsafe { Self::read_uninit(fd, buf, offset, op.with_kind(kind::READ)) }
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unaliased until completion.
    pub unsafe fn read_uninit(
        fd: RawFd,
        buf: &mut [MaybeUninit<u8>],
        offset: u64,
        op: Token,
    ) -> Self {
        Self::new(SqeInner::Read {
            fd,
            ptr: buf.as_mut_ptr().cast(),
            len: buf.len() as u32,
            offset,
            ud: op,
        })
    }

    pub fn read_fixed_file_uninit(
        slot: FdSlot,
        buf: &mut [MaybeUninit<u8>],
        offset: u64,
        op: Token,
    ) -> Self {
        Self::new(SqeInner::ReadFixed {
            slot,
            ptr: buf.as_mut_ptr().cast(),
            len: buf.len() as u32,
            offset,
            ud: op,
        })
    }

    pub fn stat_path(path: *const libc::c_char, stat: *mut libc::stat, op: Token) -> Self {
        Self::new(SqeInner::StatPath {
            path,
            stat,
            ud: op.with_kind(kind::STAT),
        })
    }

    pub fn stat_fd(fd: RawFd, stat: *mut libc::stat, op: Token) -> Self {
        Self::new(SqeInner::StatFd {
            fd,
            stat,
            ud: op.with_kind(kind::STAT),
        })
    }

    /// # Safety
    /// Both descriptors must stay open until completion.
    pub unsafe fn splice_raw(
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        _flags: u32,
        op: Token,
    ) -> Self {
        Self::splice(fd_in, off_in, fd_out, off_out, len, op)
    }

    fn splice(fd_in: RawFd, off_in: i64, fd_out: RawFd, off_out: i64, len: u32, op: Token) -> Self {
        Self::new(SqeInner::Splice {
            fd_in,
            off_in,
            fd_out,
            off_out,
            len,
            ud: op.with_kind(kind::SPLICE),
        })
    }

    pub fn splice_to_pipe(
        fd_in: RawFd,
        off_in: i64,
        pipe_write_fd: RawFd,
        len: u32,
        op: Token,
    ) -> Self {
        Self::splice(fd_in, off_in, pipe_write_fd, -1, len, op)
    }

    /// # Safety
    /// `fd` must belong to the receiving driver and stay live until completion.
    pub unsafe fn recv_multi(fd: &Fd, _buf_group: u16, op: Token) -> Self {
        Self::new(SqeInner::RecvMulti {
            slot: fd.slot(),
            ud: op.with_kind(kind::RECV),
        })
    }

    pub const SUPPORTS_RECV_DISCARD: bool = false;

    /// # Safety
    /// `fd` must belong to the receiving driver and stay live until completion.
    pub unsafe fn recv_discard(_fd: &Fd, _remaining: u64, _op: Token) -> Self {
        unreachable!()
    }

    pub fn accept_oneshot(
        listener: &Fd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        op: Token,
    ) -> Self {
        Self::new(SqeInner::AcceptOneshot {
            listener: listener.slot(),
            addr_ptr,
            addrlen_ptr,
            ud: op.with_kind(kind::ACCEPT),
        })
    }

    pub fn recv_msg_multi(fd: &Fd, msghdr: &libc::msghdr, _buf_group: u16, op: Token) -> Self {
        Self::new(SqeInner::RecvMsgMulti {
            slot: fd.slot(),
            msghdr: msghdr as *const _,
            ud: op.with_kind(kind::RECV),
        })
    }

    pub fn send_msg(fd: &Fd, msg: &libc::msghdr, op: Token) -> Self {
        Self::new(SqeInner::SendMsg {
            slot: fd.slot(),
            msg: msg as *const _,
            ud: op.with_kind(kind::SEND),
        })
    }

    pub fn quickack(_fd: &Fd) -> Self {
        Self::new(SqeInner::Quickack)
    }

    pub fn shutdown(fd: &Fd, how: i32) -> Self {
        Self::new(SqeInner::Shutdown {
            slot: fd.slot(),
            how,
        })
    }

    pub fn cancel(target: Token, kind: u8) -> Self {
        Self::new(SqeInner::Cancel {
            target: target.with_kind(kind),
        })
    }

    pub fn interval(timer: &'static TimerSpec, op: Token) -> Self {
        Self::new(SqeInner::Interval {
            sec: timer.sec,
            nsec: timer.nsec,
            ud: op.with_kind(kind::TIMER),
        })
    }

    pub fn cancel_create(slot: FdSlot) -> Self {
        Self::new(SqeInner::CancelCreate { slot })
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

    pub fn socket_at(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        Ok(Self::new(SqeInner::SocketAt {
            domain,
            socket_type,
            protocol,
            slot,
            ud: op.with_kind(kind::SOCKET),
        }))
    }

    pub fn connect(fd: &Fd, addr_ptr: *const libc::sockaddr, addr_len: u32, op: Token) -> Self {
        Self::new(SqeInner::Connect {
            slot: fd.slot(),
            addr_ptr,
            addr_len,
            ud: op.with_kind(kind::CONNECT),
        })
    }
}
