use std::mem::size_of;
use std::os::fd::RawFd;

use super::pending::PendingCompletion;
use super::read::arm::Arm;
use super::udata::Udata;
use super::{Kqueue, TAG_WRITE_RETRY};
use crate::backend::kqueue::errno::Errno;
use crate::driver::token::{Epoch, SLOT_MASK, Token};

#[derive(Clone, Copy)]
pub(crate) struct WriteRetry {
    ud: Token,
    fd: RawFd,
    kind: WriteKind,
}

pub(crate) struct WriteRetrySlot {
    retry: Option<WriteRetry>,
    epoch: Epoch,
}

#[derive(Clone, Copy)]
pub(crate) enum WriteKind {
    Send {
        ptr: *const u8,
        len: u32,
    },
    SendMsg {
        msg: *const libc::msghdr,
    },
    Connect {
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
    },
}

pub(crate) trait Retry {
    fn clear_write_retries(&mut self);
    fn retire_write_token(&mut self, target: Token);
    fn cancel_write_inner(&mut self, target: Token) -> bool;
    fn write_retry_index(&self, target: Token) -> Option<u32>;
    fn remove_write_retry(&mut self, idx: u32) -> Option<WriteRetry>;
    fn cancel_write_retry(&mut self, fd: RawFd) -> Option<Token>;
    fn dispatch_write_retry(&mut self, idx: u32, epoch: u32);
    fn alloc_write_retry(&mut self, retry: WriteRetry) -> Option<(u32, u32)>;
    fn take_write_retry(&mut self, idx: u32, epoch: u32) -> Option<WriteRetry>;
    fn arm_write_retry(&mut self, fd: RawFd, ud: Token, kind: WriteKind) -> bool;
}

impl Retry for Kqueue {
    fn clear_write_retries(&mut self) {
        for idx in 0..self.write_retries.len() as u32 {
            self.remove_write_retry(idx);
        }
    }

    fn retire_write_token(&mut self, target: Token) {
        let Some(idx) = self.write_retry_index(target) else {
            return;
        };
        self.remove_write_retry(idx);
    }

    fn cancel_write_inner(&mut self, target: Token) -> bool {
        let Some(idx) = self.write_retry_index(target) else {
            return true;
        };
        if self.pending.is_full() {
            return false;
        }
        let Some(retry) = self.remove_write_retry(idx) else {
            return true;
        };
        self.push_pending(PendingCompletion::Write {
            ud: retry.ud,
            result: -libc::ECANCELED,
        });
        true
    }

    fn write_retry_index(&self, target: Token) -> Option<u32> {
        self.write_retries
            .iter()
            .enumerate()
            .find_map(|(idx, slot)| {
                slot.retry
                    .filter(|retry| retry.ud == target)
                    .map(|_| idx as u32)
            })
    }

    fn remove_write_retry(&mut self, idx: u32) -> Option<WriteRetry> {
        let epoch = self.write_retries[idx as usize].epoch.raw();
        let udata = Udata::pack(TAG_WRITE_RETRY, idx, epoch).into_kevent();
        let queued = self.changes.iter().any(|event| event.udata == udata);
        self.changes.retain(|event| event.udata != udata);
        let slot = &mut self.write_retries[idx as usize];
        let retry = slot.retry.take()?;
        self.write_retry_fd.remove(&(retry.fd as usize));
        if slot.epoch.next().is_some() {
            self.write_retry_free.push(idx);
        }
        if !queued {
            self.disarm_filter(retry.fd, libc::EVFILT_WRITE);
        }
        Some(retry)
    }

    fn cancel_write_retry(&mut self, fd: RawFd) -> Option<Token> {
        let idx = *self.write_retry_fd.get(&(fd as usize))?;
        self.remove_write_retry(idx).map(|retry| retry.ud)
    }

    fn dispatch_write_retry(&mut self, idx: u32, epoch: u32) {
        let Some(retry) = self.take_write_retry(idx, epoch) else {
            return;
        };
        self.write_retry_fd.remove(&(retry.fd as usize));
        let result: i32 = match retry.kind {
            WriteKind::Send { ptr, len } => {
                let rc = unsafe { libc::send(retry.fd, ptr.cast(), len as usize, 0) };
                if rc == -1 {
                    -Errno::last().raw()
                } else {
                    rc as i32
                }
            }
            WriteKind::SendMsg { msg } => {
                let rc = unsafe { libc::sendmsg(retry.fd, msg, 0) };
                if rc == -1 {
                    -Errno::last().raw()
                } else {
                    rc as i32
                }
            }
            WriteKind::Connect { addr_ptr, addr_len } => {
                let mut err = 0 as libc::c_int;
                let mut len = size_of::<libc::c_int>() as libc::socklen_t;
                let rc = unsafe {
                    libc::getsockopt(
                        retry.fd,
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut err as *mut libc::c_int).cast(),
                        &mut len,
                    )
                };
                if rc == 0 && (err == libc::EINPROGRESS || err == libc::EALREADY) {
                    let rc =
                        unsafe { libc::connect(retry.fd, addr_ptr, addr_len as libc::socklen_t) };
                    if rc == 0 {
                        0
                    } else {
                        let errno = Errno::last();
                        if errno.raw() == libc::EINPROGRESS || errno.raw() == libc::EALREADY {
                            let _ = self.arm_write_retry(
                                retry.fd,
                                retry.ud,
                                WriteKind::Connect { addr_ptr, addr_len },
                            );
                            return;
                        }
                        if errno.raw() == libc::EISCONN {
                            0
                        } else {
                            -errno.raw()
                        }
                    }
                } else if rc == 0 && err == 0 {
                    0
                } else if rc == 0 {
                    -err
                } else {
                    -Errno::last().raw()
                }
            }
        };
        self.push_pending(PendingCompletion::Write {
            ud: retry.ud,
            result,
        });
    }

    fn alloc_write_retry(&mut self, retry: WriteRetry) -> Option<(u32, u32)> {
        while let Some(idx) = self.write_retry_free.pop() {
            let slot = &mut self.write_retries[idx as usize];
            let Some(epoch) = slot.epoch.next() else {
                continue;
            };
            slot.epoch = epoch;
            slot.retry = Some(retry);
            return Some((idx, epoch.raw()));
        }
        let idx = u32::try_from(self.write_retries.len()).ok()?;
        if idx as u64 > SLOT_MASK || self.write_retries.len() == self.write_retries.capacity() {
            return None;
        }
        self.write_retries.push(WriteRetrySlot {
            retry: Some(retry),
            epoch: Epoch::INITIAL,
        });
        Some((idx, Epoch::INITIAL.raw()))
    }

    fn take_write_retry(&mut self, idx: u32, epoch: u32) -> Option<WriteRetry> {
        let slot = self.write_retries.get_mut(idx as usize)?;
        if slot.epoch.raw() != epoch {
            return None;
        }
        let retry = slot.retry.take()?;
        if slot.epoch.next().is_some() {
            self.write_retry_free.push(idx);
        }
        Some(retry)
    }

    fn arm_write_retry(&mut self, fd: RawFd, ud: Token, kind: WriteKind) -> bool {
        if self.write_retry_fd.contains_key(&(fd as usize)) {
            return false;
        }
        let retry = WriteRetry { ud, fd, kind };
        let Some((idx, epoch)) = self.alloc_write_retry(retry) else {
            return false;
        };
        if !self.write_retry_fd.try_insert(fd as usize, idx) {
            self.take_write_retry(idx, epoch);
            return false;
        }
        let udata = Udata::pack(TAG_WRITE_RETRY, idx, epoch);
        self.changes.push(libc::kevent {
            ident: fd as libc::uintptr_t,
            filter: libc::EVFILT_WRITE,
            flags: libc::EV_ADD | libc::EV_CLEAR | libc::EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
        true
    }
}
