use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::time::Duration;

use o3::marker::ThreadBound;

use crate::driver::token::Token;
use crate::io::fd::{Fd, FdSlot};
use crate::driver::token::kind::ACCEPT;
use crate::driver::token::kind::CONNECT;
use crate::driver::token::kind::OPEN;
use crate::driver::token::kind::READ;
use crate::driver::token::kind::RECV;
use crate::driver::token::kind::SEND;
use crate::driver::token::kind::SOCKET;
use crate::driver::token::kind::STAT;
use crate::driver::token::kind::TIMER;
use crate::driver::token::kind::WRITE;
use libc::c_char;
use std::slice::from_raw_parts_mut;
use libc::msghdr;
use libc::sockaddr;
use libc::socklen_t;
use libc::stat;

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
        path: *const c_char,
        flags: i32,
        mode: u32,
        ud: Token,
    },
    Read {
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
        ud: Token,
    },
    StatPath {
        path: *const c_char,
        stat: *mut stat,
        ud: Token,
    },
    StatFd {
        fd: RawFd,
        stat: *mut stat,
        ud: Token,
    },
    SendMsg {
        slot: FdSlot,
        msg: *const msghdr,
        ud: Token,
    },
    AcceptOneshot {
        listener: FdSlot,
        addr_ptr: *mut sockaddr,
        addrlen_ptr: *mut socklen_t,
        ud: Token,
    },
    RecvMulti {
        slot: FdSlot,
        ud: Token,
    },
    RecvMsgMulti {
        slot: FdSlot,
        msghdr: *const msghdr,
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
        addr_ptr: *const sockaddr,
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
            ud: op.with_kind(SEND),
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
            ud: op.with_kind(WRITE),
        })
    }

    pub fn openat(dir: RawFd, path: *const c_char, flags: i32, mode: u32, op: Token) -> Self {
        Self::new(SqeInner::OpenAt {
            dir,
            path,
            flags,
            mode,
            ud: op.with_kind(OPEN),
        })
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unaliased until completion.
    pub unsafe fn read(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Self {
        let buf = unsafe {
            from_raw_parts_mut(buf.as_mut_ptr().cast::<MaybeUninit<u8>>(), buf.len())
        };
        unsafe { Self::read_uninit(fd, buf, offset, op.with_kind(READ)) }
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

    pub fn stat_path(path: *const c_char, stat: *mut stat, op: Token) -> Self {
        Self::new(SqeInner::StatPath {
            path,
            stat,
            ud: op.with_kind(STAT),
        })
    }

    pub fn stat_fd(fd: RawFd, stat: *mut stat, op: Token) -> Self {
        Self::new(SqeInner::StatFd {
            fd,
            stat,
            ud: op.with_kind(STAT),
        })
    }

    /// # Safety
    /// `fd` must belong to the receiving driver and stay live until completion.
    pub unsafe fn recv_multi(fd: &Fd, _buf_group: u16, op: Token) -> Self {
        Self::new(SqeInner::RecvMulti {
            slot: fd.slot(),
            ud: op.with_kind(RECV),
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
        addr_ptr: *mut sockaddr,
        addrlen_ptr: *mut socklen_t,
        op: Token,
    ) -> Self {
        Self::new(SqeInner::AcceptOneshot {
            listener: listener.slot(),
            addr_ptr,
            addrlen_ptr,
            ud: op.with_kind(ACCEPT),
        })
    }

    pub fn recv_msg_multi(fd: &Fd, msghdr: &msghdr, _buf_group: u16, op: Token) -> Self {
        Self::new(SqeInner::RecvMsgMulti {
            slot: fd.slot(),
            msghdr: msghdr as *const _,
            ud: op.with_kind(RECV),
        })
    }

    pub fn send_msg(fd: &Fd, msg: &msghdr, op: Token) -> Self {
        Self::new(SqeInner::SendMsg {
            slot: fd.slot(),
            msg: msg as *const _,
            ud: op.with_kind(SEND),
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
            ud: op.with_kind(TIMER),
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
            ud: op.with_kind(SOCKET),
        }))
    }

    pub fn connect(fd: &Fd, addr_ptr: *const sockaddr, addr_len: u32, op: Token) -> Self {
        Self::new(SqeInner::Connect {
            slot: fd.slot(),
            addr_ptr,
            addr_len,
            ud: op.with_kind(CONNECT),
        })
    }
}
