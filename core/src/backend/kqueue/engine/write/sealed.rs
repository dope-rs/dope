use std::{io, os::fd, process, ptr};

use o3::collections;

use crate::{
    backend::{
        self,
        kqueue::{
            engine::{event, table},
            errno,
        },
    },
    driver::flight,
    io::transfer,
};

struct Record {
    fd: fd::RawFd,
    kind: Kind,
    active_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
struct Target(flight::raw::Echo);

impl Target {
    fn new(key: flight::raw::Echo) -> Self {
        Self(key)
    }

    fn key(self) -> usize {
        self.0.raw() as usize
    }
}

pub(in crate::backend::kqueue) struct State {
    active: Vec<Target>,
    by_target: table::raw::Map<Record>,
    by_fd: table::raw::Map<Target>,
    limit: usize,
}

impl State {
    pub(in crate::backend::kqueue) fn try_with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            active: collections::VecExt::try_vec_with_capacity(capacity)?,
            by_target: table::raw::Map::try_with_capacity(capacity)?,
            by_fd: table::raw::Map::try_with_capacity(capacity)?,
            limit: capacity,
        })
    }

    fn reserve(&mut self, target: Target, fd: fd::RawFd, kind: Kind) -> Option<Reservation<'_>> {
        if self.by_fd.contains_key(&(fd as usize))
            || self.by_target.contains_key(&target.key())
            || self.active.len() == self.limit
        {
            return None;
        }
        let active_index = self.active.len();
        if !self.by_target.try_insert(
            target.key(),
            Record {
                fd,
                kind,
                active_index,
            },
        ) {
            return None;
        }
        if !self.by_fd.try_insert(fd as usize, target) {
            self.by_target.remove(&target.key());
            return None;
        }
        self.active.push(target);
        Some(Reservation {
            state: self,
            target,
            committed: false,
        })
    }

    fn take(&mut self, target: Target) -> Option<Record> {
        let retry = self.by_target.remove(&target.key())?;
        let mapped = self.by_fd.remove(&(retry.fd as usize));
        debug_assert_eq!(mapped, Some(target));

        let index = retry.active_index;
        let removed = self.active.swap_remove(index);
        debug_assert_eq!(removed, target);
        if let Some(moved) = self.active.get(index).copied() {
            let Some(record) = self.by_target.get_mut(&moved.key()) else {
                process::abort();
            };
            record.active_index = index;
        }
        Some(retry)
    }
}

struct Reservation<'a> {
    state: &'a mut State,
    target: Target,
    committed: bool,
}

impl Reservation<'_> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed && self.state.take(self.target).is_none() {
            process::abort();
        }
    }
}

pub(in crate::backend::kqueue::engine) enum Data {
    Send { ptr: *const u8, len: transfer::Len },
    SendMsg { msg: *const libc::msghdr },
}

pub(in crate::backend::kqueue::engine) enum Kind {
    Data(Data),
    Connect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Attempt {
    Complete(i32),
    WouldBlock,
}

impl Attempt {
    fn from_result(result: isize) -> Self {
        if result >= 0 {
            debug_assert!(result <= i32::MAX as isize);
            Self::Complete(result as i32)
        } else {
            Self::from_errno(errno::Errno::last())
        }
    }

    const fn from_errno(errno: errno::Errno) -> Self {
        if errno.is_block() {
            Self::WouldBlock
        } else {
            Self::Complete(-errno.raw())
        }
    }
}

const _: () = assert!(matches!(
    Attempt::from_errno(errno::Errno(libc::EAGAIN)),
    Attempt::WouldBlock
));
const _: () = assert!(matches!(
    Attempt::from_errno(errno::Errno(libc::EWOULDBLOCK)),
    Attempt::WouldBlock
));
const _: () = assert!(matches!(
    Attempt::from_errno(errno::Errno(libc::EPIPE)),
    Attempt::Complete(result) if result == -libc::EPIPE
));

impl Data {
    fn attempt(&self, fd: fd::RawFd) -> Attempt {
        let result = match self {
            Self::Send { ptr, len } => unsafe {
                use libc::send;
                send(fd, (*ptr).cast(), len.into_usize(), 0)
            },
            Self::SendMsg { msg } => unsafe {
                use libc::sendmsg;
                sendmsg(fd, *msg, 0)
            },
        };
        Attempt::from_result(result)
    }

    fn retry(&self, fd: fd::RawFd, _credit: event::Credit<'_>) -> Attempt {
        self.attempt(fd)
    }
}

#[repr(transparent)]
pub(in crate::backend::kqueue) struct Retry<'a> {
    backend: &'a mut backend::Kqueue,
}

impl<'a> Retry<'a> {
    pub(in crate::backend::kqueue) fn new(backend: &'a mut backend::Kqueue) -> Self {
        Self { backend }
    }
}

impl Retry<'_> {
    pub(crate) fn quiesce_write_retries(&mut self) {
        while let Some(target) = self.backend.retries.active.last().copied() {
            let Some(record) = self.backend.retries.take(target) else {
                process::abort();
            };
            assert!(self.backend.poll.changes.try_upsert(libc::kevent {
                ident: record.fd as libc::uintptr_t,
                filter: libc::EVFILT_WRITE,
                flags: libc::EV_DELETE,
                fflags: 0,
                data: 0,
                udata: ptr::null_mut(),
            }));
        }
    }

    pub(crate) fn cancel_write_inner(&mut self, target: flight::raw::Echo) -> bool {
        use libc::ECANCELED;
        let target = Target::new(target);
        let Some(record) = self.backend.retries.by_target.get(&target.key()) else {
            return true;
        };
        if self.backend.pending.is_full() {
            return false;
        }
        let fd = record.fd;
        let queued = self
            .backend
            .poll
            .changes
            .remove(fd as libc::uintptr_t, libc::EVFILT_WRITE);
        if !queued && !self.queue_delete(fd) {
            return false;
        }
        let Some(retry) = self.backend.retries.take(target) else {
            process::abort();
        };
        let completion = match retry.kind {
            Kind::Data(_) => event::Completion::Send {
                ud: target.0,
                result: -ECANCELED,
            },
            Kind::Connect => event::Completion::Connect {
                ud: target.0,
                result: -ECANCELED,
            },
        };
        self.backend.push_pending(completion);
        true
    }

    fn queue_delete(&mut self, fd: fd::RawFd) -> bool {
        use std::ptr::null_mut;

        use libc::{EV_DELETE, EVFILT_WRITE, kevent, uintptr_t};
        self.backend.poll.changes.try_upsert(kevent {
            ident: fd as uintptr_t,
            filter: EVFILT_WRITE,
            flags: EV_DELETE,
            fflags: 0,
            data: 0,
            udata: null_mut(),
        })
    }

    pub(crate) fn cancel_write_retry(&mut self, fd: fd::RawFd) -> Option<flight::raw::Echo> {
        let target = *self.backend.retries.by_fd.get(&(fd as usize))?;
        self.backend
            .poll
            .changes
            .remove(fd as libc::uintptr_t, libc::EVFILT_WRITE);
        self.backend.retries.take(target).map(|_| target.0)
    }

    pub(crate) fn dispatch_write_retry(
        &mut self,
        target: flight::raw::Echo,
        credit: event::Credit<'_>,
    ) {
        let target = Target::new(target);
        let Some(retry) = self.backend.retries.take(target) else {
            return;
        };
        match retry.kind {
            Kind::Data(write) => match write.retry(retry.fd, credit) {
                Attempt::Complete(result) => {
                    self.backend.push_pending(event::Completion::Send {
                        ud: target.0,
                        result,
                    });
                }
                Attempt::WouldBlock => {
                    if !self.arm_write_retry(retry.fd, target.0, Kind::Data(write)) {
                        self.backend.push_pending(event::Completion::Send {
                            ud: target.0,
                            result: -libc::ENOBUFS,
                        });
                    }
                }
            },
            Kind::Connect => {
                use std::mem::size_of;

                use libc::{EALREADY, EINPROGRESS, c_int, socklen_t};
                let _credit = credit;
                let mut err = 0 as c_int;
                let mut len = size_of::<c_int>() as socklen_t;
                let rc = unsafe {
                    use libc::{SO_ERROR, SOL_SOCKET, getsockopt};
                    getsockopt(
                        retry.fd,
                        SOL_SOCKET,
                        SO_ERROR,
                        (&mut err as *mut c_int).cast(),
                        &mut len,
                    )
                };
                if rc == 0 && (err == EINPROGRESS || err == EALREADY) {
                    if self.arm_write_retry(retry.fd, target.0, Kind::Connect) {
                        return;
                    }
                    self.backend.push_pending(event::Completion::Connect {
                        ud: target.0,
                        result: -libc::ENOBUFS,
                    });
                    return;
                }
                let result = if rc == 0 && err == 0 {
                    0
                } else if rc == 0 {
                    -err
                } else {
                    -errno::Errno::last().raw()
                };
                self.backend.push_pending(event::Completion::Connect {
                    ud: target.0,
                    result,
                });
            }
        }
    }

    pub(in crate::backend::kqueue::engine) fn submit_data_write(
        &mut self,
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        write: Data,
    ) -> bool {
        if self.backend.retries.by_fd.contains_key(&(fd as usize)) {
            return false;
        }
        self.drive_data_write(fd, ud, write)
    }

    fn drive_data_write(&mut self, fd: fd::RawFd, ud: flight::raw::Echo, write: Data) -> bool {
        match write.attempt(fd) {
            Attempt::Complete(result) => {
                self.backend
                    .push_pending(event::Completion::Send { ud, result });
                true
            }
            Attempt::WouldBlock => self.arm_write_retry(fd, ud, Kind::Data(write)),
        }
    }

    pub(in crate::backend::kqueue::engine) fn arm_write_retry(
        &mut self,
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        kind: Kind,
    ) -> bool {
        use libc::{EV_ADD, EV_CLEAR, EV_ONESHOT, kevent, uintptr_t};
        let target = Target::new(ud);
        let backend = &mut *self.backend;
        let Some(reservation) = backend.retries.reserve(target, fd, kind) else {
            return false;
        };
        if !backend.poll.changes.try_upsert(kevent {
            ident: fd as uintptr_t,
            filter: libc::EVFILT_WRITE,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: target.0.raw() as usize as *mut libc::c_void,
        }) {
            return false;
        }
        reservation.commit();
        true
    }
}
