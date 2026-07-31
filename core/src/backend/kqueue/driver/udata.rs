use libc::c_void;

use crate::driver::token::{
    EPOCH_MASK, KIND_SHIFT, ROUTE_SHIFT, SLOT_BITS, SLOT_MASK, Token,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Udata(u64);

pub(super) enum Event {
    Accept { key: ReadKey, epoch: u32 },
    Recv { key: ReadKey, epoch: u32 },
    RecvMsg { key: ReadKey, epoch: u32 },
    WriteRetry { index: u32, epoch: u32 },
    Shutdown,
}

#[derive(Clone, Copy)]
pub(super) struct ReadKey {
    route: u8,
    slot: u32,
}

impl ReadKey {
    pub(super) const fn index(self) -> usize {
        ((self.route as usize) << SLOT_BITS) | (self.slot as usize)
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Tag {
    Accept = 1,
    Recv = 2,
    RecvMsg = 3,
    WriteRetry = 4,
    Shutdown = 5,
}

impl Udata {
    const fn pack(tag: Tag, slot: u32, epoch: u32) -> Self {
        let raw = (epoch as u64 & EPOCH_MASK)
            | ((slot as u64 & SLOT_MASK) << SLOT_BITS)
            | ((tag as u64) << KIND_SHIFT);
        Self(raw)
    }

    const fn from_token(token: Token, tag: Tag) -> Self {
        let base = Self::pack(tag, token.slot().raw(), token.epoch_raw());
        Self(base.0 | ((token.route() as u64) << ROUTE_SHIFT))
    }

    pub(super) const fn read_key(token: Token) -> usize {
        ReadKey {
            route: token.route(),
            slot: token.slot().raw(),
        }
        .index()
    }

    pub(super) const fn accept(token: Token) -> Self {
        Self::from_token(token, Tag::Accept)
    }

    pub(super) const fn recv(token: Token) -> Self {
        Self::from_token(token, Tag::Recv)
    }

    pub(super) const fn recv_msg(token: Token) -> Self {
        Self::from_token(token, Tag::RecvMsg)
    }

    pub(super) const fn write_retry(index: u32, epoch: u32) -> Self {
        Self::pack(Tag::WriteRetry, index, epoch)
    }

    pub(crate) const fn shutdown() -> Self {
        Self::pack(Tag::Shutdown, 0, 0)
    }

    pub(super) fn from_kevent(p: *mut c_void) -> Self {
        Self(p as usize as u64)
    }

    pub(crate) fn into_kevent(self) -> *mut c_void {
        self.0 as usize as *mut c_void
    }

    pub(super) fn decode(self) -> Option<Event> {
        let epoch = (self.0 & EPOCH_MASK) as u32;
        let slot = ((self.0 >> SLOT_BITS) & SLOT_MASK) as u32;
        let tag = (self.0 >> KIND_SHIFT) as u8;
        let route = (self.0 >> ROUTE_SHIFT) as u8;
        let key = ReadKey { route, slot };

        match tag {
            tag if tag == Tag::Accept as u8 => Some(Event::Accept { key, epoch }),
            tag if tag == Tag::Recv as u8 => Some(Event::Recv { key, epoch }),
            tag if tag == Tag::RecvMsg as u8 => Some(Event::RecvMsg { key, epoch }),
            tag if tag == Tag::WriteRetry as u8 => Some(Event::WriteRetry {
                index: slot,
                epoch,
            }),
            tag if tag == Tag::Shutdown as u8 => Some(Event::Shutdown),
            _ => None,
        }
    }
}
