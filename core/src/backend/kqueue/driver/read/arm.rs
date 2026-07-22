use std::io::{self, Error};
use std::os::fd::RawFd;
use std::ptr::null_mut;

use super::super::pending::PendingCompletion;
use super::super::udata::Udata;
use super::super::{Kqueue, TAG_ACCEPT, TAG_RECV, TAG_RECV_MSG};
use super::{AcceptSlot, ReadSlot, RecvMsgSlot, SlotHeader};
use crate::driver::token::Token;
use crate::io::fd::FdSlot;

pub(crate) enum RecvKind {
    Bytes,
    Message(*const libc::msghdr),
}

pub(crate) trait Arm {
    fn set_fd_nonblocking(raw: RawFd) -> io::Result<()>;
    fn arm_read_multi(&mut self, raw: RawFd, udata: Udata) -> io::Result<()>;
    fn re_enable_read(&mut self, raw: RawFd, udata: Udata);
    fn disarm_filter(&mut self, fd: RawFd, filter: i16);
    fn arm_accept_oneshot_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
    ) -> bool;
    fn cancel_accept_inner(&mut self, ud: Token) -> bool;
    fn arm_recv_inner(&mut self, ud: Token, slot: FdSlot, kind: RecvKind) -> bool;
    fn arm_recv_multi_inner(&mut self, ud: Token, slot: FdSlot) -> bool;
    unsafe fn arm_recv_msg_multi_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool;
    fn cancel_recv_inner(&mut self, ud: Token) -> bool;
}

impl Arm for Kqueue {
    fn set_fd_nonblocking(raw: RawFd) -> io::Result<()> {
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(Error::last_os_error());
        }
        let rc = unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if rc < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn arm_read_multi(&mut self, raw: RawFd, udata: Udata) -> io::Result<()> {
        Self::set_fd_nonblocking(raw)?;
        self.changes.push(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
        Ok(())
    }

    fn re_enable_read(&mut self, raw: RawFd, udata: Udata) {
        self.changes.push(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
    }

    fn disarm_filter(&mut self, fd: RawFd, filter: i16) {
        if filter == 0 {
            return;
        }
        self.changes.push(libc::kevent {
            ident: fd as libc::uintptr_t,
            filter,
            flags: libc::EV_DELETE,
            fflags: 0,
            data: 0,
            udata: null_mut(),
        });
        self.flush_changes_if_full();
    }

    fn arm_accept_oneshot_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
    ) -> bool {
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let epoch = ud.epoch().raw();
        if !self.insert_read(
            slot_idx,
            ReadSlot::Accept(AcceptSlot {
                hdr: SlotHeader {
                    fd,
                    epoch,
                    armed: true,
                    resume_queued: false,
                    ud,
                },
                addr_ptr,
                addrlen_ptr,
                oneshot: true,
            }),
        ) {
            return false;
        }
        if self
            .arm_read_multi(fd, Udata::from_token(ud, TAG_ACCEPT))
            .is_err()
        {
            self.remove_read(slot_idx);
            return false;
        }
        true
    }

    fn cancel_accept_inner(&mut self, ud: Token) -> bool {
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let Some(slot) = self
            .read_slots
            .get_mut(&slot_idx)
            .and_then(ReadSlot::accept_mut)
            .filter(|slot| slot.hdr.armed && slot.hdr.ud == ud)
        else {
            return true;
        };
        if self.pending.is_full() {
            return false;
        }
        slot.hdr.armed = false;
        let fd = slot.hdr.fd;
        let ud = slot.hdr.ud;
        self.disarm_filter(fd, libc::EVFILT_READ);
        self.push_pending(PendingCompletion::Accept {
            ud,
            result: -libc::ECANCELED,
            more: false,
        });
        true
    }

    fn arm_recv_inner(&mut self, ud: Token, slot: FdSlot, kind: RecvKind) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -libc::EBADF,
                more: false,
                bid: None,
            });
            return true;
        };
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let epoch = ud.epoch().raw();
        let header = SlotHeader {
            fd: raw,
            epoch,
            armed: true,
            resume_queued: false,
            ud,
        };
        let (tag, read) = match kind {
            RecvKind::Bytes => (TAG_RECV, ReadSlot::Recv(header)),
            RecvKind::Message(msg_template) => (
                TAG_RECV_MSG,
                ReadSlot::RecvMsg(RecvMsgSlot {
                    hdr: header,
                    msg_template,
                }),
            ),
        };
        if !self.insert_read(slot_idx, read) {
            return false;
        }
        if self
            .arm_read_multi(raw, Udata::from_token(ud, tag))
            .is_err()
        {
            self.remove_read(slot_idx);
            return false;
        }
        true
    }

    fn arm_recv_multi_inner(&mut self, ud: Token, slot: FdSlot) -> bool {
        self.arm_recv_inner(ud, slot, RecvKind::Bytes)
    }

    unsafe fn arm_recv_msg_multi_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool {
        self.arm_recv_inner(ud, slot, RecvKind::Message(msg))
    }

    fn cancel_recv_inner(&mut self, ud: Token) -> bool {
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let needed = usize::from(
            self.read_slots
                .get(&slot_idx)
                .is_some_and(|slot| match slot {
                    ReadSlot::Recv(slot) => slot.armed && slot.ud == ud,
                    ReadSlot::RecvMsg(slot) => slot.hdr.armed && slot.hdr.ud == ud,
                    ReadSlot::Accept(_) => false,
                }),
        );
        if self.pending.remaining_capacity() < needed {
            return false;
        }
        if let Some(slot) = self
            .read_slots
            .get_mut(&slot_idx)
            .and_then(ReadSlot::recv_mut)
            .filter(|slot| slot.armed && slot.ud == ud)
        {
            slot.armed = false;
            let fd = slot.fd;
            let ud = slot.ud;
            self.disarm_filter(fd, libc::EVFILT_READ);
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -libc::ECANCELED,
                more: false,
                bid: None,
            });
        }
        if let Some(slot) = self
            .read_slots
            .get_mut(&slot_idx)
            .and_then(ReadSlot::recv_msg_mut)
            .filter(|slot| slot.hdr.armed && slot.hdr.ud == ud)
        {
            slot.hdr.armed = false;
            let fd = slot.hdr.fd;
            let ud = slot.hdr.ud;
            self.disarm_filter(fd, libc::EVFILT_READ);
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -libc::ECANCELED,
                more: false,
                bid: None,
            });
        }
        true
    }
}
