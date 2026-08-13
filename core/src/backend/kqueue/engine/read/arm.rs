use std::{io, os::fd, ptr};

use crate::{
    backend::{
        self,
        kqueue::engine::{event, read},
    },
    driver::flight,
    io::fd::handles,
};

enum RecvKind {
    Bytes,
    Message,
}

#[repr(transparent)]
pub(crate) struct Arm<'a> {
    backend: &'a mut backend::Kqueue,
}

impl<'a> Arm<'a> {
    pub(in crate::backend::kqueue::engine) fn new(backend: &'a mut backend::Kqueue) -> Self {
        Self { backend }
    }
}

impl Arm<'_> {
    fn arm_read_multi(&mut self, raw: fd::RawFd, udata: event::Udata) -> io::Result<()> {
        if !self.backend.poll.changes.try_upsert(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        }) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "dope: kqueue changelist capacity exhausted",
            ));
        }
        Ok(())
    }

    pub(in crate::backend::kqueue::engine) fn re_enable_read(
        &mut self,
        raw: fd::RawFd,
        udata: event::Udata,
    ) {
        assert!(self.backend.poll.changes.try_upsert(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        }));
    }

    pub(in crate::backend::kqueue::engine) fn disarm_filter(&mut self, fd: fd::RawFd, filter: i16) {
        use std::ptr::null_mut;
        if filter == 0 {
            return;
        }
        assert!(self.backend.poll.changes.try_upsert(libc::kevent {
            ident: fd as libc::uintptr_t,
            filter,
            flags: libc::EV_DELETE,
            fflags: 0,
            data: 0,
            udata: null_mut(),
        }));
    }

    pub(in crate::backend::kqueue::engine) fn arm_accept_oneshot_inner(
        &mut self,
        ud: flight::raw::Echo,
        fixed: handles::FixedSlot,
        fd: fd::RawFd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
    ) -> bool {
        let slot = read::Slot::Accept(read::AcceptSlot {
            hdr: read::SlotHeader { fd, fixed, ud },
            addr_ptr,
            addrlen_ptr,
            oneshot: true,
        });
        let Some(read) = self.backend.reads.install(slot) else {
            return false;
        };
        if self.arm_read_multi(fd, event::Udata::accept(ud)).is_err() {
            let removed = self.backend.reads.remove_active(read);
            debug_assert_eq!(removed, Some(fd));
            return false;
        }
        true
    }

    pub(in crate::backend::kqueue::engine) fn arm_accept_multishot_inner(
        &mut self,
        ud: flight::raw::Echo,
        fixed: handles::FixedSlot,
        fd: fd::RawFd,
    ) -> bool {
        let slot = read::Slot::Accept(read::AcceptSlot {
            hdr: read::SlotHeader { fd, fixed, ud },
            addr_ptr: ptr::null_mut(),
            addrlen_ptr: ptr::null_mut(),
            oneshot: false,
        });
        let Some(read) = self.backend.reads.install(slot) else {
            return false;
        };
        if self.arm_read_multi(fd, event::Udata::accept(ud)).is_err() {
            let removed = self.backend.reads.remove_active(read);
            debug_assert_eq!(removed, Some(fd));
            return false;
        }
        true
    }

    pub(in crate::backend::kqueue::engine) fn cancel_accept_inner(
        &mut self,
        ud: flight::raw::Echo,
    ) -> bool {
        self.cancel_read_inner(ud, read::Family::Accept)
    }

    fn arm_recv_inner(
        &mut self,
        ud: flight::raw::Echo,
        slot: handles::FixedSlot,
        kind: RecvKind,
    ) -> bool {
        let Some(raw) = self.backend.files.raw(slot) else {
            self.backend.push_pending(event::Completion::RecvControl {
                ud,
                result: -libc::EBADF,
                more: false,
            });
            return true;
        };
        let header = read::SlotHeader {
            fd: raw,
            fixed: slot,
            ud,
        };
        let (udata, read) = match kind {
            RecvKind::Bytes => (event::Udata::recv(ud), read::Slot::Recv(header)),
            RecvKind::Message => (event::Udata::recv_msg(ud), read::Slot::RecvMsg(header)),
        };
        let Some(read) = self.backend.reads.install(read) else {
            return false;
        };
        if self.arm_read_multi(raw, udata).is_err() {
            let removed = self.backend.reads.remove_active(read);
            debug_assert_eq!(removed, Some(raw));
            return false;
        }
        true
    }

    pub(in crate::backend::kqueue::engine) fn arm_recv_multi_inner(
        &mut self,
        ud: flight::raw::Echo,
        slot: handles::FixedSlot,
    ) -> bool {
        self.arm_recv_inner(ud, slot, RecvKind::Bytes)
    }

    pub(in crate::backend::kqueue::engine) fn arm_recv_msg_multi_inner(
        &mut self,
        ud: flight::raw::Echo,
        slot: handles::FixedSlot,
    ) -> bool {
        self.arm_recv_inner(ud, slot, RecvKind::Message)
    }

    pub(in crate::backend::kqueue::engine) fn cancel_recv_inner(
        &mut self,
        ud: flight::raw::Echo,
    ) -> bool {
        self.cancel_read_inner(ud, read::Family::Recv)
    }

    fn cancel_read_inner(&mut self, ud: flight::raw::Echo, family: read::Family) -> bool {
        let slot_idx = event::Udata::read_key(ud);
        let found = self.backend.reads.slots.get(&slot_idx).is_some_and(|slot| {
            let header = slot.header();
            slot.family() == family && header.ud == ud
        });
        if !found {
            return true;
        }
        if self.backend.pending.is_full() {
            return false;
        }
        let Some(slot) = self.backend.reads.remove(slot_idx) else {
            return true;
        };
        let fd = slot.header().fd;
        self.disarm_filter(fd, libc::EVFILT_READ);
        match family {
            read::Family::Accept => self.backend.push_pending(event::Completion::AcceptFailure {
                ud,
                errno: libc::ECANCELED,
                more: false,
            }),
            read::Family::Recv => self.backend.push_pending(event::Completion::RecvControl {
                ud,
                result: -libc::ECANCELED,
                more: false,
            }),
        }
        true
    }
}
