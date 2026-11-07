use std::mem;

use libc::socklen_t;
use o3::{base::marker::ThreadBound, buffer::bytes};

use crate::{io, io::socket::addr};

/// Native requests retaining every input address through their borrow.
#[repr(transparent)]
pub struct Borrowed<'a, I>(Retained<'a, I>, ThreadBound);

pub enum Retained<'a, I> {
    Accept {
        listener: I,
        addr: &'a mut libc::sockaddr_storage,
        len: &'a mut socklen_t,
    },
    Send {
        slot: I,
        input: bytes::Prefix<'a, { i32::MAX as usize }>,
    },
    SendMsg {
        slot: I,
        msg: &'a io::socket::msg::Header,
    },
    Connect {
        slot: I,
        addr: addr::Addr<'a>,
    },
}

impl<'a, I> Borrowed<'a, I> {
    pub fn send(slot: I, input: bytes::Prefix<'a, { i32::MAX as usize }>) -> Self {
        Self(Retained::Send { slot, input }, ThreadBound::NEW)
    }

    pub fn send_msg<M: io::raw::Message<'a>>(slot: I, message: M) -> Self {
        Self(
            Retained::SendMsg {
                slot,
                msg: message.header(),
            },
            ThreadBound::NEW,
        )
    }

    pub fn accept(
        listener: I,
        address: &'a mut libc::sockaddr_storage,
        len: &'a mut socklen_t,
    ) -> Self {
        const CAPACITY: socklen_t = {
            assert!(mem::size_of::<libc::sockaddr_storage>() <= socklen_t::MAX as usize);
            mem::size_of::<libc::sockaddr_storage>() as socklen_t
        };
        *len = CAPACITY;
        Self(
            Retained::Accept {
                listener,
                addr: address,
                len,
            },
            ThreadBound::NEW,
        )
    }

    pub fn connect(slot: I, addr: addr::Addr<'a>) -> Self {
        Self(Retained::Connect { slot, addr }, ThreadBound::NEW)
    }

    pub fn into_inner(self) -> Retained<'a, I> {
        self.0
    }
}
