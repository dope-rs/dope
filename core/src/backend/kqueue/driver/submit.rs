use std::os::fd::{IntoRawFd, RawFd};

use super::Kqueue;
use super::pending::PendingCompletion;
use super::read::arm::Arm;
use super::retry::{Retry, WriteKind};
use crate::platform::raw::abi::PlatformAbi;
use crate::backend::kqueue::errno::Errno;
use crate::driver::Driver;
use crate::driver::token::{KIND_SHIFT, Token, kind};
use crate::io::fd::FdSlot;
use crate::io::ffi::Handle;

use super::SPLICE_BOUNCE;

pub(crate) trait Submit {
    fn cancel_inner(&mut self, target: Token) -> bool;
    fn submit_send_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        ptr: *const u8,
        len: u32,
    ) -> bool;
    fn complete_io(&mut self, ud: Token, rc: isize) -> bool;
    fn complete_create(&mut self, ud: Token, result: i32, slot: FdSlot) -> bool;
    fn submit_write_fd_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
    ) -> bool;
    fn submit_openat_inner(
        &mut self,
        ud: Token,
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        fixed: Option<FdSlot>,
    ) -> bool;
    fn submit_read_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
    ) -> bool;
    fn submit_splice_inner(
        &mut self,
        ud: Token,
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
    ) -> bool;
    fn submit_socket_at(
        &mut self,
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        ud: Token,
    ) -> bool;
    fn submit_connect(
        &mut self,
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: Token,
    ) -> bool;
    unsafe fn submit_send_msg_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool;
}

impl Submit for Kqueue {
    fn cancel_inner(&mut self, target: Token) -> bool {
        match (target.raw() >> KIND_SHIFT) as u8 {
            kind::ACCEPT => self.cancel_accept_inner(target),
            kind::RECV | kind::RECV_DISCARD => self.cancel_recv_inner(target),
            kind::SEND | kind::CONNECT => self.cancel_write_inner(target),
            kind::TIMER => {
                self.changes.push(libc::kevent {
                    ident: target.raw() as libc::uintptr_t,
                    filter: libc::EVFILT_TIMER,
                    flags: libc::EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                });
                self.flush_changes_if_full();
                true
            }
            _ => true,
        }
    }

    fn submit_send_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        ptr: *const u8,
        len: u32,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        if self.write_retry_fd.contains_key(&(raw as usize)) {
            return false;
        }
        let n = unsafe { libc::send(raw, ptr.cast(), len as usize, 0) };
        if n >= 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: n as i32,
            });
            return true;
        }
        let errno = Errno::last();
        if errno.is_block() {
            return self.arm_write_retry(raw, ud, WriteKind::Send { ptr, len });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno.raw(),
        });
        true
    }

    fn complete_io(&mut self, ud: Token, rc: isize) -> bool {
        let result = if rc < 0 {
            -Errno::last().raw()
        } else {
            rc as i32
        };
        self.push_pending(PendingCompletion::Write { ud, result });
        true
    }

    fn complete_create(&mut self, ud: Token, result: i32, slot: FdSlot) -> bool {
        self.push_pending(PendingCompletion::Create {
            ud,
            result,
            slot: Some(slot),
        });
        true
    }

    fn submit_write_fd_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
    ) -> bool {
        let n = unsafe { libc::pwrite(fd, ptr.cast(), len as usize, offset as libc::off_t) };
        self.complete_io(ud, n)
    }

    fn submit_openat_inner(
        &mut self,
        ud: Token,
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        fixed: Option<FdSlot>,
    ) -> bool {
        let oflag = if fixed.is_some() {
            flags | libc::O_CLOEXEC
        } else {
            flags
        };
        let fd = unsafe { libc::openat(dir, path, oflag, mode as libc::c_uint) };
        if fd < 0 {
            let result = -Errno::last().raw();
            return match fixed {
                Some(slot) => self.complete_create(ud, result, slot),
                None => self.complete_io(ud, fd as isize),
            };
        }
        if let Some(slot) = fixed {
            let _ = self.register_raw_fd(slot.raw(), fd);
            return self.complete_create(ud, 0, slot);
        }
        self.complete_io(ud, fd as isize)
    }

    fn submit_read_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        ptr: *mut u8,
        len: u32,
        offset: u64,
    ) -> bool {
        let n = unsafe { libc::pread(fd, ptr.cast(), len as usize, offset as libc::off_t) };
        self.complete_io(ud, n)
    }

    fn submit_splice_inner(
        &mut self,
        ud: Token,
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
    ) -> bool {
        let cap = (len as usize).min(SPLICE_BOUNCE);
        let buf = &mut self.splice_buf[..cap];
        let n = unsafe {
            if off_in < 0 {
                libc::read(fd_in, buf.as_mut_ptr().cast(), cap)
            } else {
                libc::pread(fd_in, buf.as_mut_ptr().cast(), cap, off_in as libc::off_t)
            }
        };
        if n <= 0 {
            return self.complete_io(ud, n);
        }
        let w = unsafe {
            if off_out < 0 {
                libc::write(fd_out, buf.as_ptr().cast(), n as usize)
            } else {
                libc::pwrite(
                    fd_out,
                    buf.as_ptr().cast(),
                    n as usize,
                    off_out as libc::off_t,
                )
            }
        };
        self.complete_io(ud, w)
    }

    fn submit_socket_at(
        &mut self,
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        ud: Token,
    ) -> bool {
        let raw = unsafe { libc::socket(domain, socket_type, protocol) };
        if raw < 0 {
            return self.complete_create(ud, -Errno::last().raw(), slot);
        }
        let sock = Handle::take(raw);
        let result = if sock.set_cloexec().is_err()
            || sock.set_nonblocking().is_err()
            || Driver::set_no_sigpipe(&sock).is_err()
        {
            -Errno::last().raw()
        } else {
            let raw = sock.into_raw_fd();
            match self.register_raw_fd(slot.raw(), raw) {
                Ok(()) => 0,
                Err(_) => {
                    self.close_raw(raw);
                    -libc::EMFILE
                }
            }
        };
        self.complete_create(ud, result, slot)
    }

    fn submit_connect(
        &mut self,
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: Token,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        let rc = unsafe { libc::connect(raw, addr_ptr, addr_len as libc::socklen_t) };
        if rc == 0 {
            self.push_pending(PendingCompletion::Write { ud, result: 0 });
            return true;
        }
        let errno = Errno::last();
        if errno.raw() == libc::EINPROGRESS || errno.is_block() {
            return self.arm_write_retry(raw, ud, WriteKind::Connect { addr_ptr, addr_len });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno.raw(),
        });
        true
    }

    unsafe fn submit_send_msg_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        if self.write_retry_fd.contains_key(&(raw as usize)) {
            return false;
        }
        let n = unsafe { libc::sendmsg(raw, msg, 0) };
        if n >= 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: n as i32,
            });
            return true;
        }
        let errno = Errno::last();
        if errno.is_block() {
            return self.arm_write_retry(raw, ud, WriteKind::SendMsg { msg });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno.raw(),
        });
        true
    }
}
