use std::{mem, ptr};

use dope_sys::io::{reactor::complete, request, socket};
use o3::base::marker::ThreadBound;

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
        let mut entry = Self(Fields::default(), ThreadBound::NEW);
        entry.initialize(opcode, fd);
        entry
    }

    fn initialize(&mut self, opcode: op::Code, fd: sq::Descriptor) {
        let (raw, fixed) = fd.encode();
        opcode.write(&mut self.0.opcode);
        self.0.fd = raw;
        if fixed {
            self.0.flags = Flags::FIXED_FILE.raw();
        }
    }

    pub(crate) fn retained<'a>(request: request::Borrowed<'a, sq::Fixed>) -> sq::Borrowed<'a> {
        let mut entry = Self(Fields::default(), ThreadBound::NEW);
        entry.write(request);
        sq::Borrowed::new(entry)
    }

    #[inline]
    pub(super) fn write(&mut self, request: request::Borrowed<'_, sq::Fixed>) {
        use request::Retained;

        self.0 = Fields::default();
        match request.into_inner() {
            Retained::Send { slot, input } => {
                self.initialize(buffers::SEND, sq::Descriptor::Fixed(slot));
                let input = input.as_slice();
                self.0.address = input.as_ptr() as u64;
                self.0.length = input.len() as u32;
                self.0.options = libc::MSG_NOSIGNAL.unsigned_abs();
            }
            Retained::SendMsg { slot, msg } => {
                self.initialize(messages::SEND, sq::Descriptor::Fixed(slot));
                self.0.address = ptr::from_ref(msg) as u64;
                self.0.length = 1;
                self.0.options = libc::MSG_NOSIGNAL.unsigned_abs();
            }
            Retained::Accept {
                listener,
                addr,
                len,
            } => {
                self.initialize(sockets::ACCEPT, sq::Descriptor::Fixed(listener));
                self.0.index = ring::ffi::FILE_INDEX_ALLOC;
                self.0.address = ptr::from_mut(addr).cast::<libc::sockaddr>() as u64;
                self.0.offset = (ptr::from_mut(len) as u64).to_ne_bytes();
            }
            Retained::Connect { slot, addr } => {
                self.initialize(sockets::CONNECT, sq::Descriptor::Fixed(slot));
                let (addr, len) = addr.parts();
                self.0.address = addr as u64;
                self.0.offset = (len.get() as u64).to_ne_bytes();
            }
        }
    }

    pub(crate) fn fields(&mut self) -> &mut Fields {
        &mut self.0
    }

    pub(crate) fn flags(mut self, flags: Flags) -> Self {
        self.0.flags |= flags.raw();
        self
    }

    pub(crate) fn user_data<T: complete::raw::Token>(self, token: T) -> sq::Owned<T> {
        let mut entry = self;
        entry.fields().token(token);
        sq::Owned::new(entry)
    }

    pub(crate) fn bind<'a, T: complete::raw::Token>(mut self, token: T) -> sq::Submit<'a, T> {
        self.fields().token(token);
        sq::Submit::new(self)
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

impl Fields {
    pub(crate) fn token<T: complete::raw::Token>(&mut self, token: T) {
        self.user_data = sq::raw::Word::encode(token).get();
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
