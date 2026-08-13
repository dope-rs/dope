use std::{marker, os::fd, ptr};

use crate::{
    backend::{
        self,
        kqueue::{
            engine::{event, read, write},
            errno,
        },
    },
    driver::flight,
    io::socket,
};

mod outcome;

/// Stack-local recvmsg graph whose writable buffers cannot outlive the call.
struct RecvCall<'a> {
    header: libc::msghdr,
    iov: libc::iovec,
    buffers: marker::PhantomData<&'a mut [u8]>,
}

impl<'a> RecvCall<'a> {
    fn new(name: &'a mut [u8], payload: &'a mut [u8], name_len: libc::socklen_t) -> Self {
        Self {
            header: libc::msghdr {
                msg_name: name.as_mut_ptr().cast(),
                msg_namelen: name_len,
                msg_iov: ptr::null_mut(),
                msg_iovlen: 1,
                msg_control: ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
            iov: libc::iovec {
                iov_base: payload.as_mut_ptr().cast(),
                iov_len: payload.len(),
            },
            buffers: marker::PhantomData,
        }
    }

    fn recv(&mut self, fd: fd::RawFd) -> isize {
        self.header.msg_iov = ptr::from_mut(&mut self.iov);
        unsafe { libc::recvmsg(fd, ptr::from_mut(&mut self.header), 0) as isize }
    }

    fn flags(&self) -> libc::c_int {
        self.header.msg_flags
    }
}

#[repr(transparent)]
pub(crate) struct Dispatch<'a> {
    backend: &'a mut backend::Kqueue,
}

impl<'a> Dispatch<'a> {
    pub(in crate::backend::kqueue) fn new(backend: &'a mut backend::Kqueue) -> Self {
        Self { backend }
    }
}

impl Dispatch<'_> {
    pub(crate) fn dispatch_event(&mut self, ev: event::Kernel<'_>, credit: event::Credit<'_>) {
        use libc::EVFILT_WRITE;
        let filter = ev.filter();
        let error = ev.error();
        let data = super::KernelData::decode(ev.into_raw());
        if filter == EVFILT_WRITE {
            if let super::KernelData::Public(target) = data {
                write::Retry::new(self.backend).dispatch_write_retry(target, credit);
            }
            return;
        }
        match data {
            super::KernelData::Public(key) => {
                if let Some(read) = self.backend.reads.id(key) {
                    self.dispatch_read(read, filter, error, credit)
                }
            }
            super::KernelData::Shutdown => self.backend.push_pending(event::Completion::Shutdown),
            super::KernelData::Empty => {}
        }
    }

    fn dispatch_read(
        &mut self,
        read: read::Id,
        filter: i16,
        error: Option<i32>,
        credit: event::Credit<'_>,
    ) {
        let Some(op) = self.backend.reads.operation(read) else {
            return;
        };
        debug_assert_eq!(filter, libc::EVFILT_READ);
        if let Some(errno) = error {
            self.terminate_read(read);
            self.push_read_error(op, errno);
            return;
        }
        self.drive_read(read, op, credit);
    }

    fn drive_read(&mut self, read: read::Id, op: read::Operation, credit: event::Credit<'_>) {
        let outcome = op.visit(
            (&mut *self, credit),
            |(dispatch, credit), fd, ud, addr_ptr, addrlen_ptr, oneshot| {
                dispatch.drain_accept(fd, ud, addr_ptr, addrlen_ptr, oneshot, credit)
            },
            |(dispatch, credit), fd, ud| dispatch.drain_recv(fd, ud, credit),
            |(dispatch, credit), fd, ud| dispatch.drain_recv_msg(fd, ud, credit),
        );
        match outcome {
            outcome::Outcome::Rearm => {
                read::Arm::new(self.backend).re_enable_read(op.fd(), op.udata());
            }
            outcome::Outcome::Yield => self.queue_resume(read),
            outcome::Outcome::Terminal => self.terminate_read(read),
        }
    }

    fn terminate_read(&mut self, read: read::Id) {
        let Some(fd) = self.backend.reads.remove_active(read) else {
            return;
        };
        read::Arm::new(self.backend).disarm_filter(fd, libc::EVFILT_READ);
    }

    fn push_read_error(&mut self, op: read::Operation, errno: i32) {
        op.visit(
            (&mut *self, errno),
            |(dispatch, errno), _, ud, _, _, _| {
                dispatch
                    .backend
                    .push_pending(event::Completion::AcceptFailure {
                        ud,
                        errno,
                        more: false,
                    });
            },
            |(dispatch, errno), _, ud| {
                dispatch
                    .backend
                    .push_pending(event::Completion::RecvControl {
                        ud,
                        result: -errno,
                        more: false,
                    });
            },
            |(dispatch, errno), _, ud| {
                dispatch
                    .backend
                    .push_pending(event::Completion::RecvControl {
                        ud,
                        result: -errno,
                        more: false,
                    });
            },
        );
    }

    fn drain_accept(
        &mut self,
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        oneshot: bool,
        _credit: event::Credit<'_>,
    ) -> outcome::Outcome {
        if self.backend.pending.is_full() {
            return outcome::Outcome::Yield;
        }
        if !addrlen_ptr.is_null() {
            unsafe {
                use std::mem::size_of;

                use libc::sockaddr_storage;
                *addrlen_ptr = size_of::<sockaddr_storage>() as libc::socklen_t;
            }
        }
        let raw = unsafe {
            use libc::accept;
            accept(fd, addr_ptr, addrlen_ptr)
        };
        if raw >= 0 {
            use crate::backend::kqueue::descriptor;
            // SAFETY: accept returned a fresh owned descriptor.
            let accepted = unsafe { fd::FromRawFd::from_raw_fd(raw) };
            let Some(vacancy) = self.backend.files.vacant_accept() else {
                use libc::EMFILE;
                drop(accepted);
                self.backend.push_pending(event::Completion::AcceptFailure {
                    ud,
                    errno: EMFILE,
                    more: !oneshot,
                });
                return if oneshot {
                    outcome::Outcome::Terminal
                } else {
                    outcome::Outcome::Rearm
                };
            };
            let Ok(accepted) = descriptor::Handle::from_inheriting_accept(accepted) else {
                return outcome::Outcome::Yield;
            };
            let accepted = vacancy.insert(accepted);
            self.backend.push_pending(event::Completion::AcceptSuccess {
                ud,
                accepted,
                more: !oneshot,
            });
            return if oneshot {
                outcome::Outcome::Terminal
            } else {
                outcome::Outcome::Yield
            };
        }
        let errno = errno::Errno::last();
        if errno.is_block() {
            return outcome::Outcome::Rearm;
        }
        self.backend.push_pending(event::Completion::AcceptFailure {
            ud,
            errno: errno.raw(),
            more: !oneshot,
        });
        if oneshot {
            outcome::Outcome::Terminal
        } else {
            outcome::Outcome::Rearm
        }
    }

    fn drain_recv(
        &mut self,
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        _credit: event::Credit<'_>,
    ) -> outcome::Outcome {
        if self.backend.pending.is_full() {
            return outcome::Outcome::Yield;
        }
        let Some(mut buffer) = self.backend.recv.take() else {
            return outcome::Outcome::Yield;
        };
        let cap = buffer.capacity();
        let ptr = buffer.spare_mut().as_mut_ptr();
        let n = unsafe {
            use libc::recv;
            recv(fd, ptr.cast(), cap, 0)
        };
        if n > 0 {
            self.backend.push_pending(event::Completion::RecvData {
                ud,
                len: n as u32,
                more: true,
                buffer,
            });
            return outcome::Outcome::Yield;
        }
        if n == 0 {
            drop(buffer);
            self.backend.push_pending(event::Completion::RecvControl {
                ud,
                result: 0,
                more: false,
            });
            return outcome::Outcome::Terminal;
        }
        let errno = errno::Errno::last();
        drop(buffer);
        if errno.is_block() {
            return outcome::Outcome::Rearm;
        }
        self.backend.push_pending(event::Completion::RecvControl {
            ud,
            result: -errno.raw(),
            more: true,
        });
        outcome::Outcome::Rearm
    }

    fn drain_recv_msg(
        &mut self,
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        _credit: event::Credit<'_>,
    ) -> outcome::Outcome {
        let namelen = socket::Addr::STORAGE_CAPACITY;
        let name_len = namelen as libc::socklen_t;
        if self.backend.pending.is_full() {
            return outcome::Outcome::Yield;
        }
        let Some(mut buffer) = self.backend.recv.take() else {
            return outcome::Outcome::Yield;
        };
        let cap = buffer.capacity();
        if cap <= namelen {
            use libc::ENOBUFS;
            drop(buffer);
            self.backend.push_pending(event::Completion::RecvControl {
                ud,
                result: -ENOBUFS,
                more: true,
            });
            return outcome::Outcome::Rearm;
        }
        let (name, payload) = buffer.spare_mut().split_at_mut(namelen);
        let mut call = RecvCall::new(name, payload, name_len);
        let n = call.recv(fd);
        let flags = call.flags();
        if n > 0 {
            use libc::MSG_TRUNC;
            if flags & MSG_TRUNC != 0 {
                drop(buffer);
                return outcome::Outcome::Yield;
            }
            let total = namelen + n as usize;
            self.backend.push_pending(event::Completion::RecvData {
                ud,
                len: total as u32,
                more: true,
                buffer,
            });
            return outcome::Outcome::Yield;
        }
        if n == 0 {
            self.backend.push_pending(event::Completion::RecvData {
                ud,
                len: namelen as u32,
                more: true,
                buffer,
            });
            return outcome::Outcome::Rearm;
        }
        let errno = errno::Errno::last();
        drop(buffer);
        if errno.is_block() {
            return outcome::Outcome::Rearm;
        }
        self.backend.push_pending(event::Completion::RecvControl {
            ud,
            result: -errno.raw(),
            more: true,
        });
        outcome::Outcome::Rearm
    }
    fn queue_resume(&mut self, resume: read::Id) {
        self.backend.reads.queue_resume(resume);
    }

    fn take_resume(&mut self, resume: read::Id) -> Option<read::Operation> {
        self.backend.reads.take_resume(resume)
    }

    pub(crate) fn resume_pending_with(
        &mut self,
        budget: &mut event::Budget<'_, '_, event::ResumeLane>,
    ) {
        while self.backend.reads.resume_len() != 0 {
            if self.backend.pending.is_full() {
                break;
            }
            let Some(credit) = budget.take() else {
                break;
            };
            let Some(item) = self.backend.reads.pop_resume() else {
                break;
            };
            if let Some(op) = self.take_resume(item) {
                self.drive_read(item, op, credit);
            }
        }
    }
}
