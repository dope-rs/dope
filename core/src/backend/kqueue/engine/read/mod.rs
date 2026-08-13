use std::{io, os::fd, ptr};

mod arm;

pub(in crate::backend::kqueue::engine) use arm::Arm;
use o3::collections::queue::slot;

use crate::{
    backend::kqueue::engine::{event, table},
    driver::flight,
    io::fd::handles,
};

pub(in crate::backend::kqueue) struct Registry {
    slots: table::raw::Map<Slot>,
    by_fd: table::raw::Map<usize>,
    resume: slot::Fifo<Id>,
}

impl Registry {
    pub(in crate::backend::kqueue) fn try_with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            slots: table::raw::Map::try_with_capacity(capacity)?,
            by_fd: table::raw::Map::try_with_capacity(capacity)?,
            resume: slot::Fifo::try_with_capacity(capacity)?,
        })
    }

    fn install(&mut self, slot: Slot) -> Option<Id> {
        let read = slot.id()?;
        if slot.header().fixed.raw() as usize >= self.resume.capacity() {
            return None;
        }
        let fd = slot.header().fd as usize;
        if !self.by_fd.try_insert(fd, read.key) {
            return None;
        }
        if self.slots.try_insert(read.key, slot) {
            return Some(read);
        }
        let removed = self.by_fd.remove(&fd);
        debug_assert_eq!(removed, Some(read.key));
        None
    }

    fn remove(&mut self, key: usize) -> Option<Slot> {
        let slot = self.slots.remove(&key)?;
        if let Some(read) = self.resume.remove(slot.header().fixed.raw() as usize) {
            debug_assert!(slot.matches(read));
        }
        let fd = slot.header().fd as usize;
        if self
            .by_fd
            .get(&fd)
            .is_some_and(|registered| *registered == key)
        {
            self.by_fd.remove(&fd);
        }
        Some(slot)
    }

    pub(in crate::backend::kqueue::engine) fn remove_active(
        &mut self,
        read: Id,
    ) -> Option<fd::RawFd> {
        let slot = self.slots.get(&read.key)?;
        if !slot.matches(read) {
            return None;
        }
        self.remove(read.key).map(|slot| slot.header().fd)
    }

    pub(in crate::backend::kqueue::engine) fn remove_fd(
        &mut self,
        raw: fd::RawFd,
    ) -> Option<flight::raw::Echo> {
        let key = self.by_fd.get(&(raw as usize)).copied()?;
        self.remove(key).map(|slot| slot.header().ud)
    }

    pub(in crate::backend::kqueue::engine) fn operation(&self, read: Id) -> Option<Operation> {
        let slot = self.slots.get(&read.key)?;
        slot.matches(read).then(|| slot.operation())
    }

    pub(in crate::backend::kqueue::engine) fn id(&self, key: flight::raw::Echo) -> Option<Id> {
        self.slots.get(&(key.raw() as usize)).and_then(Slot::id)
    }

    pub(in crate::backend::kqueue::engine) fn queue_resume(&mut self, read: Id) {
        let Some(slot) = self.slots.get(&read.key) else {
            return;
        };
        if !slot.matches(read) {
            return;
        }
        let Some(entry) = self.resume.vacant_entry(slot.header().fixed.raw() as usize) else {
            return;
        };
        entry.push_back(read);
    }

    pub(in crate::backend::kqueue::engine) fn take_resume(&self, read: Id) -> Option<Operation> {
        let slot = self.slots.get(&read.key)?;
        slot.matches(read).then(|| slot.operation())
    }

    pub(in crate::backend::kqueue::engine) fn resume_len(&self) -> usize {
        self.resume.len()
    }

    pub(in crate::backend::kqueue::engine) fn pop_resume(&mut self) -> Option<Id> {
        self.resume.pop_front()
    }

    pub(crate) fn clear_resume(&mut self) {
        self.resume.clear();
    }

    pub(crate) fn has_pending_resume(&self) -> bool {
        !self.resume.is_empty()
    }

    pub(in crate::backend::kqueue) fn quiesce(&mut self, changes: &mut event::Changes) {
        for slot in self.slots.values() {
            assert!(changes.try_upsert(libc::kevent {
                ident: slot.header().fd as libc::uintptr_t,
                filter: libc::EVFILT_READ,
                flags: libc::EV_DELETE,
                fflags: 0,
                data: 0,
                udata: ptr::null_mut(),
            }));
        }
        self.slots.clear();
        self.by_fd.clear();
        self.clear_resume();
    }
}

struct SlotHeader {
    fd: fd::RawFd,
    fixed: handles::FixedSlot,
    ud: flight::raw::Echo,
}

const _: () = assert!(std::mem::size_of::<SlotHeader>() == 2 * std::mem::size_of::<u64>());

struct AcceptSlot {
    hdr: SlotHeader,
    addr_ptr: *mut libc::sockaddr,
    addrlen_ptr: *mut libc::socklen_t,
    oneshot: bool,
}

enum Slot {
    Accept(AcceptSlot),
    Recv(SlotHeader),
    RecvMsg(SlotHeader),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Accept,
    Recv,
}

impl Slot {
    fn kind(&self) -> Kind {
        match self {
            Self::Accept(_) => Kind::Accept,
            Self::Recv(_) => Kind::Recv,
            Self::RecvMsg(_) => Kind::RecvMsg,
        }
    }

    fn family(&self) -> Family {
        match self {
            Self::Accept(_) => Family::Accept,
            Self::Recv(_) | Self::RecvMsg(_) => Family::Recv,
        }
    }

    fn header(&self) -> &SlotHeader {
        match self {
            Self::Accept(slot) => &slot.hdr,
            Self::Recv(slot) => slot,
            Self::RecvMsg(slot) => slot,
        }
    }

    fn id(&self) -> Option<Id> {
        let header = self.header();
        Some(Id {
            key: event::Udata::read_key(header.ud),
            kind: self.kind(),
        })
    }

    fn matches(&self, read: Id) -> bool {
        self.id() == Some(read)
    }

    fn operation(&self) -> Operation {
        Operation(match self {
            Self::Accept(slot) => OperationKind::Accept {
                fd: slot.hdr.fd,
                ud: slot.hdr.ud,
                addr_ptr: slot.addr_ptr,
                addrlen_ptr: slot.addrlen_ptr,
                oneshot: slot.oneshot,
            },
            Self::Recv(slot) => OperationKind::Recv {
                fd: slot.fd,
                ud: slot.ud,
            },
            Self::RecvMsg(slot) => OperationKind::RecvMsg {
                fd: slot.fd,
                ud: slot.ud,
            },
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Accept,
    Recv,
    RecvMsg,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::backend::kqueue::engine) struct Id {
    key: usize,
    kind: Kind,
}

impl Id {}

#[derive(Clone, Copy)]
pub(in crate::backend::kqueue::engine) struct Operation(OperationKind);

#[derive(Clone, Copy)]
enum OperationKind {
    Accept {
        fd: fd::RawFd,
        ud: flight::raw::Echo,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        oneshot: bool,
    },
    Recv {
        fd: fd::RawFd,
        ud: flight::raw::Echo,
    },
    RecvMsg {
        fd: fd::RawFd,
        ud: flight::raw::Echo,
    },
}

impl Operation {
    pub(in crate::backend::kqueue::engine) fn visit<Context, Output>(
        self,
        context: Context,
        accept: impl FnOnce(
            Context,
            fd::RawFd,
            flight::raw::Echo,
            *mut libc::sockaddr,
            *mut libc::socklen_t,
            bool,
        ) -> Output,
        recv: impl FnOnce(Context, fd::RawFd, flight::raw::Echo) -> Output,
        recv_msg: impl FnOnce(Context, fd::RawFd, flight::raw::Echo) -> Output,
    ) -> Output {
        match self.0 {
            OperationKind::Accept {
                fd,
                ud,
                addr_ptr,
                addrlen_ptr,
                oneshot,
            } => accept(context, fd, ud, addr_ptr, addrlen_ptr, oneshot),
            OperationKind::Recv { fd, ud } => recv(context, fd, ud),
            OperationKind::RecvMsg { fd, ud } => recv_msg(context, fd, ud),
        }
    }

    pub(in crate::backend::kqueue::engine) const fn fd(self) -> fd::RawFd {
        match self.0 {
            OperationKind::Accept { fd, .. }
            | OperationKind::Recv { fd, .. }
            | OperationKind::RecvMsg { fd, .. } => fd,
        }
    }

    pub(in crate::backend::kqueue::engine) fn udata(self) -> event::Udata {
        match self.0 {
            OperationKind::Accept { ud, .. } => event::Udata::accept(ud),
            OperationKind::Recv { ud, .. } => event::Udata::recv(ud),
            OperationKind::RecvMsg { ud, .. } => event::Udata::recv_msg(ud),
        }
    }
}
