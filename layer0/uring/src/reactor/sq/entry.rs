use std::{mem, ptr};

use dope_sys::io::{reactor::complete, request, socket};
use o3::base::marker::ThreadBound;

use crate::reactor::{
    op::{self, buffers, messages, sockets},
    ring,
    sq::{self, Flags},
};

#[derive(Clone, Debug)]
#[repr(C, align(8))]
pub struct Entry([u8; 64], ThreadBound);

pub(crate) struct Fields<'a>(&'a mut Entry);

impl Entry {
    pub(crate) const NOP: op::Code = op::Code::new(ring::ffi::NOP);

    pub(crate) fn new(opcode: op::Code, fd: sq::Descriptor) -> Self {
        let (raw, fixed) = fd.encode();
        let mut entry = Self([0; 64], ThreadBound::NEW);
        opcode.write(&mut entry.0[0]);
        entry.0[4..8].copy_from_slice(&raw.to_ne_bytes());
        if fixed {
            entry.0[1] = Flags::FIXED_FILE.raw();
        }
        entry
    }

    pub(crate) fn fields(&mut self) -> Fields<'_> {
        Fields(self)
    }

    pub(crate) fn flags(mut self, flags: Flags) -> Self {
        self.0[1] |= flags.raw();
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
    type Borrowed<'a> = sq::Borrowed<'a>;

    fn socket(slot: Self::Outbound, domain: Self::Domain) -> Self {
        let mut entry = Self::new(sockets::CREATE, sq::Descriptor::File(domain.raw()));
        let mut fields = entry.fields();
        fields.offset(libc::SOCK_STREAM as u64);
        fields.index(slot.encode());
        entry
    }

    fn accept_multi(listener: Self::Target) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = sockets::accept(listener);
        entry.fields().priority(ffi::ACCEPT_MULTISHOT);
        entry
    }

    fn recv<const GROUP: u16>(slot: Self::Target) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = Self::new(buffers::RECEIVE, sq::Descriptor::Fixed(slot))
            .flags(sq::Flags::BUFFER_SELECT);
        let mut fields = entry.fields();
        fields.priority(ffi::RECEIVE_MULTISHOT);
        fields.group(GROUP);
        entry
    }

    fn recv_msg<const GROUP: u16>(datagram: Self::Datagram) -> Self {
        use crate::reactor::ring::ffi;

        let mut entry = Self::new(messages::RECEIVE, sq::Descriptor::Fixed(datagram))
            .flags(sq::Flags::BUFFER_SELECT);
        let mut fields = entry.fields();
        fields.address(ptr::from_ref(op::RECEIVE_HEADER) as u64);
        fields.length(1);
        fields.group(GROUP);
        fields.priority(ffi::RECEIVE_MULTISHOT);
        entry
    }

    fn shutdown(slot: Self::Target) -> Self {
        sockets::shutdown(slot, sockets::Direction::Write)
    }
}

impl Fields<'_> {
    pub(crate) fn token<T: complete::raw::Token>(&mut self, token: T) {
        self.write_u64(32, sq::raw::Word::encode(token).get());
    }

    pub(crate) fn offset(&mut self, value: u64) {
        self.write_u64(8, value);
    }

    pub(crate) fn address(&mut self, value: u64) {
        self.write_u64(16, value);
    }

    pub(crate) fn length(&mut self, value: u32) {
        self.write_u32(24, value);
    }

    pub(crate) fn options(&mut self, value: u32) {
        self.write_u32(28, value);
    }

    pub(crate) fn priority(&mut self, value: u16) {
        self.write_u16(2, value);
    }

    pub(crate) fn group(&mut self, value: u16) {
        self.write_u16(40, value);
    }

    pub(crate) fn index(&mut self, value: u32) {
        self.write_u32(44, value);
    }

    pub(crate) fn address3(&mut self, value: u64) {
        self.write_u64(48, value);
    }

    pub(crate) fn command(&mut self, value: u32) {
        self.write_u32(8, value);
    }

    fn write_u16(&mut self, offset: usize, value: u16) {
        self.0.0[offset..offset + 2].copy_from_slice(&value.to_ne_bytes());
    }

    fn write_u32(&mut self, offset: usize, value: u32) {
        self.0.0[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
    }

    fn write_u64(&mut self, offset: usize, value: u64) {
        self.0.0[offset..offset + 8].copy_from_slice(&value.to_ne_bytes());
    }
}

const _: () = {
    assert!(mem::size_of::<Entry>() == 64);
    assert!(mem::align_of::<Entry>() == 8);
};
