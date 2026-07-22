use std::os::fd::RawFd;

use o3::collections::FixedHashTable;

use crate::driver::token::Token;

pub(crate) mod arm;
pub(crate) mod dispatch;

pub(super) struct FixedMap<T>(FixedHashTable<(usize, T)>);

impl<T> FixedMap<T> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self(FixedHashTable::with_capacity(capacity))
    }

    fn hash(key: usize) -> u64 {
        let folded = key ^ (key >> 24);
        folded.wrapping_mul(0x9E37_79B9_7F4A_7C15usize) as u64
    }

    pub(super) fn insert(&mut self, key: usize, value: T) -> Result<Option<T>, T> {
        self.0
            .insert(Self::hash(key), (key, value), |entry| entry.0 == key)
            .map(|entry| entry.map(|(_, value)| value))
            .map_err(|(_, value)| value)
    }

    pub(super) fn try_insert(&mut self, key: usize, value: T) -> bool {
        self.0
            .try_insert(Self::hash(key), (key, value), |entry| entry.0 == key)
            .is_ok()
    }

    pub(super) fn get(&self, key: &usize) -> Option<&T> {
        self.0
            .get(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| &entry.1)
    }

    pub(super) fn get_mut(&mut self, key: &usize) -> Option<&mut T> {
        self.0
            .get_mut(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| &mut entry.1)
    }

    pub(super) fn contains_key(&self, key: &usize) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn remove(&mut self, key: &usize) -> Option<T> {
        self.0
            .remove(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| entry.1)
    }
}

pub(super) struct SlotHeader {
    pub(super) fd: RawFd,
    pub(super) epoch: u32,
    pub(super) armed: bool,
    pub(super) resume_queued: bool,
    pub(super) ud: Token,
}

impl SlotHeader {
    pub(super) fn validate(
        &mut self,
        ev: &libc::kevent,
        epoch: u32,
    ) -> Result<(RawFd, Token), Option<(Token, i32)>> {
        if self.epoch != epoch || !self.armed {
            return Err(None);
        }
        if (ev.flags & libc::EV_ERROR) != 0 && ev.data != 0 {
            self.armed = false;
            return Err(Some((self.ud, ev.data as i32)));
        }
        Ok((self.fd, self.ud))
    }
}

pub(super) struct AcceptSlot {
    pub(super) hdr: SlotHeader,
    pub(super) addr_ptr: *mut libc::sockaddr,
    pub(super) addrlen_ptr: *mut libc::socklen_t,
    pub(super) oneshot: bool,
}

pub(super) struct RecvMsgSlot {
    pub(super) hdr: SlotHeader,
    pub(super) msg_template: *const libc::msghdr,
}

pub(super) enum ReadSlot {
    Accept(AcceptSlot),
    Recv(SlotHeader),
    RecvMsg(RecvMsgSlot),
}

impl ReadSlot {
    pub(super) fn kind(&self) -> ReadKind {
        match self {
            Self::Accept(_) => ReadKind::Accept,
            Self::Recv(_) => ReadKind::Recv,
            Self::RecvMsg(_) => ReadKind::RecvMsg,
        }
    }

    pub(super) fn header(&self) -> &SlotHeader {
        match self {
            Self::Accept(slot) => &slot.hdr,
            Self::Recv(slot) => slot,
            Self::RecvMsg(slot) => &slot.hdr,
        }
    }

    pub(super) fn header_mut(&mut self) -> &mut SlotHeader {
        match self {
            Self::Accept(slot) => &mut slot.hdr,
            Self::Recv(slot) => slot,
            Self::RecvMsg(slot) => &mut slot.hdr,
        }
    }

    pub(super) fn accept(&self) -> Option<&AcceptSlot> {
        match self {
            Self::Accept(slot) => Some(slot),
            _ => None,
        }
    }

    pub(super) fn accept_mut(&mut self) -> Option<&mut AcceptSlot> {
        match self {
            Self::Accept(slot) => Some(slot),
            _ => None,
        }
    }

    pub(super) fn recv(&self) -> Option<&SlotHeader> {
        match self {
            Self::Recv(slot) => Some(slot),
            _ => None,
        }
    }

    pub(super) fn recv_mut(&mut self) -> Option<&mut SlotHeader> {
        match self {
            Self::Recv(slot) => Some(slot),
            _ => None,
        }
    }

    pub(super) fn recv_msg(&self) -> Option<&RecvMsgSlot> {
        match self {
            Self::RecvMsg(slot) => Some(slot),
            _ => None,
        }
    }

    pub(super) fn recv_msg_mut(&mut self) -> Option<&mut RecvMsgSlot> {
        match self {
            Self::RecvMsg(slot) => Some(slot),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadKind {
    Accept,
    Recv,
    RecvMsg,
}

#[derive(Clone, Copy)]
pub(crate) struct Resume {
    pub(super) key: usize,
    pub(super) epoch: u32,
    pub(super) kind: ReadKind,
}

pub(crate) enum DrainOutcome {
    Done,
    Yield,
    Closed,
}
