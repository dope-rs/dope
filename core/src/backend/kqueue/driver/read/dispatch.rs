use std::mem::size_of;
use std::os::fd::{IntoRawFd, RawFd};
use std::slice;

use super::super::pending::PendingCompletion;
use super::super::retry::Retry;
use super::super::udata::Udata;
use super::super::{Kqueue, MAX_DRAIN_PER_FD, PENDING_CAP};
use super::super::{TAG_ACCEPT, TAG_RECV, TAG_RECV_MSG, TAG_SHUTDOWN, TAG_WRITE_RETRY};
use super::arm::Arm;
use super::{DrainOutcome, ReadKind, ReadSlot, Resume};
use crate::platform::raw::abi::PlatformAbi;
use crate::backend::kqueue::errno::Errno;
use crate::driver::Driver;
use crate::driver::token::Token;
use crate::io::ffi::Handle;
use crate::io::socket::msg::IoVec;
use crate::io::socket::msg::MsgHdr;

pub(crate) trait Dispatch {
    fn dispatch_event(&mut self, ev: &libc::kevent);
    fn dispatch_accept(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent);
    fn drain_accept(
        &mut self,
        fd: RawFd,
        ud: Token,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        oneshot: bool,
    ) -> DrainOutcome;
    fn dispatch_recv(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent);
    fn drain_recv(&mut self, fd: RawFd, ud: Token) -> DrainOutcome;
    fn dispatch_recv_msg(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent);
    fn drain_recv_msg(
        &mut self,
        fd: RawFd,
        ud: Token,
        msg_tpl: *const libc::msghdr,
    ) -> DrainOutcome;
    fn queue_resume(&mut self, resume: Resume);
    fn take_resume(&mut self, resume: Resume) -> bool;
    fn resume_pending(&mut self);
}

impl Dispatch for Kqueue {
    fn dispatch_event(&mut self, ev: &libc::kevent) {
        if ev.filter == libc::EVFILT_USER {
            return;
        }
        if ev.filter == libc::EVFILT_TIMER {
            if let Some(ud) = Token::try_from_raw(ev.udata as usize as u64) {
                self.push_pending(PendingCompletion::Timer { ud });
            }
            return;
        }
        let raw = Udata::from_kevent(ev.udata);
        let (tag, route, slot, epoch) = raw.unpack();
        match tag {
            TAG_ACCEPT => self.dispatch_accept(Udata::slot_key(route, slot), epoch, ev),
            TAG_RECV => self.dispatch_recv(Udata::slot_key(route, slot), epoch, ev),
            TAG_RECV_MSG => self.dispatch_recv_msg(Udata::slot_key(route, slot), epoch, ev),
            TAG_WRITE_RETRY => self.dispatch_write_retry(slot, epoch),
            TAG_SHUTDOWN => self.push_pending(PendingCompletion::Shutdown),
            _ => {}
        }
    }

    fn dispatch_accept(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = {
            let Some(slot) = self
                .read_slots
                .get_mut(&slot_idx)
                .and_then(ReadSlot::accept_mut)
            else {
                return;
            };
            match slot.hdr.validate(ev, epoch) {
                Ok((fd, ud)) => Ok((fd, ud, slot.addr_ptr, slot.addrlen_ptr, slot.oneshot)),
                Err(e) => Err(e),
            }
        };
        let (fd, ud, addr_ptr, addrlen_ptr, oneshot) = match outcome {
            Ok(t) => t,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Accept {
                    ud,
                    result: -data,
                    more: false,
                });
                return;
            }
        };
        match self.drain_accept(fd, ud, addr_ptr, addrlen_ptr, oneshot) {
            DrainOutcome::Done => {
                if !oneshot {
                    self.re_enable_read(fd, Udata::from_token(ud, TAG_ACCEPT));
                } else if let Some(s) = self
                    .read_slots
                    .get_mut(&slot_idx)
                    .and_then(ReadSlot::accept_mut)
                {
                    s.hdr.armed = false;
                }
            }
            DrainOutcome::Yield => self.queue_resume(Resume {
                key: slot_idx,
                epoch,
                kind: ReadKind::Accept,
            }),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_accept(
        &mut self,
        fd: RawFd,
        ud: Token,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        oneshot: bool,
    ) -> DrainOutcome {
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            if !addrlen_ptr.is_null() {
                unsafe {
                    *addrlen_ptr = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                }
            }
            let raw = unsafe { libc::accept(fd, addr_ptr, addrlen_ptr) };
            let more_flag = !oneshot;
            if raw >= 0 {
                let Some(slot) = self.next_accept_slot() else {
                    self.close_raw(raw);
                    self.push_pending(PendingCompletion::Accept {
                        ud,
                        result: -libc::EMFILE,
                        more: more_flag,
                    });
                    return DrainOutcome::Done;
                };
                let accepted = Handle::take(raw);
                if accepted.set_nonblocking().is_err()
                    || accepted.set_cloexec().is_err()
                    || Driver::set_no_sigpipe(&accepted).is_err()
                {
                    continue;
                }
                let raw = accepted.into_raw_fd();
                if self.register_raw_fd(slot, raw).is_err() {
                    self.close_raw(raw);
                    self.push_pending(PendingCompletion::Accept {
                        ud,
                        result: -libc::EMFILE,
                        more: more_flag,
                    });
                    return DrainOutcome::Done;
                }
                self.push_pending(PendingCompletion::Accept {
                    ud,
                    result: slot as i32,
                    more: more_flag,
                });
                if oneshot {
                    return DrainOutcome::Done;
                }
                continue;
            }
            let errno = Errno::last();
            if errno.is_block() {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Accept {
                ud,
                result: -errno.raw(),
                more: more_flag,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn dispatch_recv(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = match self
            .read_slots
            .get_mut(&slot_idx)
            .and_then(ReadSlot::recv_mut)
        {
            Some(h) => h.validate(ev, epoch),
            None => return,
        };
        let (fd, ud) = match outcome {
            Ok(p) => p,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -data,
                    more: false,
                    bid: None,
                });
                return;
            }
        };
        match self.drain_recv(fd, ud) {
            DrainOutcome::Done => self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV)),
            DrainOutcome::Yield => self.queue_resume(Resume {
                key: slot_idx,
                epoch,
                kind: ReadKind::Recv,
            }),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_recv(&mut self, fd: RawFd, ud: Token) -> DrainOutcome {
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            let Some((bid, ptr, cap)) = self.provided.take() else {
                return DrainOutcome::Yield;
            };
            let n = unsafe { libc::recv(fd, ptr.cast(), cap, 0) };
            if n > 0 {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: n as i32,
                    more: true,
                    bid: Some(bid),
                });
                continue;
            }
            if n == 0 {
                self.provided.defer(bid);
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: 0,
                    more: false,
                    bid: None,
                });
                return DrainOutcome::Closed;
            }
            let errno = Errno::last();
            self.provided.defer(bid);
            if errno.is_block() {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -errno.raw(),
                more: true,
                bid: None,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn dispatch_recv_msg(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = {
            let Some(slot) = self
                .read_slots
                .get_mut(&slot_idx)
                .and_then(ReadSlot::recv_msg_mut)
            else {
                return;
            };
            match slot.hdr.validate(ev, epoch) {
                Ok((fd, ud)) => Ok((fd, ud, slot.msg_template)),
                Err(e) => Err(e),
            }
        };
        let (fd, ud, msg_tpl) = match outcome {
            Ok(t) => t,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -data,
                    more: false,
                    bid: None,
                });
                return;
            }
        };
        match self.drain_recv_msg(fd, ud, msg_tpl) {
            DrainOutcome::Done => self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV_MSG)),
            DrainOutcome::Yield => self.queue_resume(Resume {
                key: slot_idx,
                epoch,
                kind: ReadKind::RecvMsg,
            }),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_recv_msg(
        &mut self,
        fd: RawFd,
        ud: Token,
        msg_tpl: *const libc::msghdr,
    ) -> DrainOutcome {
        let template = unsafe { *msg_tpl };
        let namelen = template.msg_namelen as usize;
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            let Some((bid, ptr, cap)) = self.provided.take() else {
                return DrainOutcome::Yield;
            };
            if cap <= namelen {
                self.provided.defer(bid);
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -libc::ENOBUFS,
                    more: true,
                    bid: None,
                });
                return DrainOutcome::Done;
            }
            let iov = IoVec::from_mut_slice(unsafe {
                slice::from_raw_parts_mut(ptr.add(namelen), cap - namelen)
            });
            let mut local_msg = MsgHdr::empty();
            local_msg.set_name_ptr(ptr.cast(), template.msg_namelen);
            local_msg.set_iov(slice::from_ref(&iov));
            let n = unsafe { libc::recvmsg(fd, local_msg.as_mut_ptr(), 0) };
            if n > 0 {
                if local_msg.flags() & libc::MSG_TRUNC != 0 {
                    self.provided.defer(bid);
                    continue;
                }
                let total = namelen + n as usize;
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: total as i32,
                    more: true,
                    bid: Some(bid),
                });
                continue;
            }
            if n == 0 {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: namelen as i32,
                    more: true,
                    bid: Some(bid),
                });
                return DrainOutcome::Done;
            }
            let errno = Errno::last();
            self.provided.defer(bid);
            if errno.is_block() {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -errno.raw(),
                more: true,
                bid: None,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn queue_resume(&mut self, resume: Resume) {
        let Some(slot) = self.read_slots.get_mut(&resume.key) else {
            return;
        };
        if slot.kind() != resume.kind {
            return;
        }
        let header = slot.header_mut();
        if header.epoch != resume.epoch || !header.armed || header.resume_queued {
            return;
        }
        let Some(entry) = self.resume.vacant_entry() else {
            unreachable!()
        };
        header.resume_queued = true;
        entry.push_back(resume);
    }

    fn take_resume(&mut self, resume: Resume) -> bool {
        let Some(slot) = self.read_slots.get_mut(&resume.key) else {
            return false;
        };
        if slot.kind() != resume.kind {
            return false;
        }
        let header = slot.header_mut();
        if header.epoch != resume.epoch || !header.resume_queued {
            return false;
        }
        header.resume_queued = false;
        header.armed
    }

    fn resume_pending(&mut self) {
        let n = self.resume.len();
        for _ in 0..n {
            let Some(item) = self.resume.pop_front() else {
                break;
            };
            if !self.take_resume(item) {
                continue;
            }
            let slot_idx = item.key;
            match item.kind {
                ReadKind::Accept => {
                    let Some((fd, ud, addr_ptr, addrlen_ptr, oneshot)) = self
                        .read_slots
                        .get(&slot_idx)
                        .and_then(ReadSlot::accept)
                        .filter(|s| s.hdr.armed)
                        .map(|s| (s.hdr.fd, s.hdr.ud, s.addr_ptr, s.addrlen_ptr, s.oneshot))
                    else {
                        continue;
                    };
                    match self.drain_accept(fd, ud, addr_ptr, addrlen_ptr, oneshot) {
                        DrainOutcome::Done => {
                            if !oneshot {
                                self.re_enable_read(fd, Udata::from_token(ud, TAG_ACCEPT))
                            } else if let Some(s) = self
                                .read_slots
                                .get_mut(&slot_idx)
                                .and_then(ReadSlot::accept_mut)
                            {
                                s.hdr.armed = false;
                            }
                        }
                        DrainOutcome::Yield => self.queue_resume(item),
                        DrainOutcome::Closed => {}
                    }
                }
                ReadKind::Recv => {
                    let Some((fd, ud)) = self
                        .read_slots
                        .get(&slot_idx)
                        .and_then(ReadSlot::recv)
                        .filter(|h| h.armed)
                        .map(|h| (h.fd, h.ud))
                    else {
                        continue;
                    };
                    match self.drain_recv(fd, ud) {
                        DrainOutcome::Done => {
                            self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV))
                        }
                        DrainOutcome::Yield => self.queue_resume(item),
                        DrainOutcome::Closed => {}
                    }
                }
                ReadKind::RecvMsg => {
                    let Some((fd, ud, tpl)) = self
                        .read_slots
                        .get(&slot_idx)
                        .and_then(ReadSlot::recv_msg)
                        .filter(|s| s.hdr.armed)
                        .map(|s| (s.hdr.fd, s.hdr.ud, s.msg_template))
                    else {
                        continue;
                    };
                    match self.drain_recv_msg(fd, ud, tpl) {
                        DrainOutcome::Done => {
                            self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV_MSG))
                        }
                        DrainOutcome::Yield => self.queue_resume(item),
                        DrainOutcome::Closed => {}
                    }
                }
            }
        }
    }
}
