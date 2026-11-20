use std::{mem, ptr};

use dope_sys::io::{reactor::complete, request, socket};
use o3::{base::marker::ThreadBound, buffer::bytes};

use crate::reactor::{
    op::{self, buffers, messages, sockets},
    ring,
    sq::{self, Flags},
};

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Entry(Fields, ThreadBound);

#[derive(Clone, Debug, Default)]
#[repr(C, align(8))]
pub(crate) struct Fields {
    opcode: u8,
    flags: u8,
    pub(crate) priority: u16,
    fd: i32,
    pub(crate) offset: [u8; 8],
    pub(crate) address: u64,
    pub(crate) length: u32,
    pub(crate) options: u32,
    user_data: u64,
    pub(crate) group: u16,
    personality: u16,
    pub(crate) index: u32,
    pub(crate) address3: u64,
    padding: u64,
}

impl Entry {
    pub(crate) const NOP: op::Code = op::Code::new(ring::ffi::NOP);

    pub(crate) fn new(opcode: op::Code, fd: sq::Descriptor) -> Self {
        Self(Fields::new(opcode, fd, 0), ThreadBound::NEW)
    }

    pub(super) fn connect(deferred: sq::raw::Deferred, word: sq::raw::Word) -> Self {
        let (slot, address, length) = deferred.into_parts();
        Self(
            Fields {
                address: address as u64,
                offset: (length.get() as u64).to_ne_bytes(),
                ..Fields::fixed(sockets::CONNECT, slot, word)
            },
            ThreadBound::NEW,
        )
    }

    pub(crate) fn fields(&mut self) -> &mut Fields {
        &mut self.0
    }

    pub(crate) fn flags(mut self, flags: Flags) -> Self {
        self.0.flags |= flags.raw();
        self
    }

    pub(crate) fn user_data<T: complete::raw::Token>(self, token: T) -> sq::Owned<T> {
        sq::Owned::new(self.with_token(token))
    }

    pub(crate) fn bind<'a, T: complete::raw::Token>(self, token: T) -> sq::Submit<'a, T> {
        sq::Submit::new(self.with_token(token))
    }

    pub(super) fn with_token<T: complete::raw::Token>(mut self, token: T) -> Self {
        self.0.user_data = sq::raw::Word::encode(token).get();
        self
    }
}

impl request::Lower for Entry {
    type Target = sq::Fixed;
    type Outbound = sq::Fixed;
    type Domain = socket::Domain;
    type Datagram = sq::Fixed;

    fn socket(slot: Self::Outbound, domain: Self::Domain) -> Self {
        let mut entry = Self::new(sockets::CREATE, sq::Descriptor::File(domain.raw()));
        let fields = entry.fields();
        fields.offset = (libc::SOCK_STREAM as u64).to_ne_bytes();
        fields.index = slot.encode();
        entry
    }

    fn accept_multi(listener: Self::Target) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = sockets::accept(listener);
        entry.fields().priority = ffi::ACCEPT_MULTISHOT;
        entry
    }

    fn recv<const GROUP: u16>(slot: Self::Target) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = Self::new(buffers::RECEIVE, sq::Descriptor::Fixed(slot))
            .flags(sq::Flags::BUFFER_SELECT);
        let fields = entry.fields();
        fields.priority = ffi::RECEIVE_MULTISHOT;
        fields.group = GROUP;
        entry
    }

    fn recv_msg<const GROUP: u16>(datagram: Self::Datagram) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = Self::new(messages::RECEIVE, sq::Descriptor::Fixed(datagram))
            .flags(sq::Flags::BUFFER_SELECT);
        let fields = entry.fields();
        fields.address = ptr::from_ref(op::RECEIVE_HEADER) as u64;
        fields.length = 1;
        fields.group = GROUP;
        fields.priority = ffi::RECEIVE_MULTISHOT;
        entry
    }

    fn shutdown(slot: Self::Target) -> Self {
        sockets::shutdown(slot, dope_sys::io::socket::How::WRITE)
    }
}

impl<'a> sq::Borrowed<'a> {
    fn from_fields(fields: Fields) -> Self {
        Self::new(Entry(fields, ThreadBound::NEW))
    }

    pub(crate) fn send(
        slot: sq::Fixed,
        input: bytes::Prefix<'a, { i32::MAX as usize }>,
        word: sq::raw::Word,
    ) -> Self {
        let input = input.as_slice();
        Self::from_fields(Fields {
            address: input.as_ptr() as u64,
            length: input.len() as u32,
            options: libc::MSG_NOSIGNAL.unsigned_abs(),
            ..Fields::fixed(buffers::SEND, slot, word)
        })
    }

    pub(crate) fn send_msg(
        slot: sq::Fixed,
        message: impl dope_sys::io::raw::Message<'a>,
        word: sq::raw::Word,
    ) -> Self {
        Self::from_fields(Fields {
            address: ptr::from_ref(message.header()) as u64,
            length: 1,
            options: libc::MSG_NOSIGNAL.unsigned_abs(),
            ..Fields::fixed(messages::SEND, slot, word)
        })
    }

    pub(crate) fn accept(
        listener: sq::Fixed,
        peer: &'a mut socket::addr::Storage,
        word: sq::raw::Word,
    ) -> Self {
        let (address, length) = peer.prepare_output();
        Self::from_fields(Fields {
            address: ptr::from_mut(address).cast::<libc::sockaddr>() as u64,
            offset: (ptr::from_mut(length) as u64).to_ne_bytes(),
            index: ring::ffi::FILE_INDEX_ALLOC,
            ..Fields::fixed(sockets::ACCEPT, listener, word)
        })
    }
}

impl Fields {
    fn fixed(opcode: op::Code, slot: sq::Fixed, word: sq::raw::Word) -> Self {
        Self::new(opcode, sq::Descriptor::Fixed(slot), word.get())
    }

    fn new(opcode: op::Code, descriptor: sq::Descriptor, user_data: u64) -> Self {
        let (fd, fixed) = descriptor.encode();
        Self {
            opcode: opcode.raw(),
            flags: if fixed { Flags::FIXED_FILE.raw() } else { 0 },
            fd,
            user_data,
            ..Self::default()
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Entry>() == 64);
    assert!(mem::align_of::<Entry>() == 8);
    assert!(mem::offset_of!(Fields, opcode) == 0);
    assert!(mem::offset_of!(Fields, flags) == 1);
    assert!(mem::offset_of!(Fields, priority) == 2);
    assert!(mem::offset_of!(Fields, fd) == 4);
    assert!(mem::offset_of!(Fields, offset) == 8);
    assert!(mem::offset_of!(Fields, address) == 16);
    assert!(mem::offset_of!(Fields, length) == 24);
    assert!(mem::offset_of!(Fields, options) == 28);
    assert!(mem::offset_of!(Fields, user_data) == 32);
    assert!(mem::offset_of!(Fields, group) == 40);
    assert!(mem::offset_of!(Fields, personality) == 42);
    assert!(mem::offset_of!(Fields, index) == 44);
    assert!(mem::offset_of!(Fields, address3) == 48);
    assert!(mem::offset_of!(Fields, padding) == 56);
};
