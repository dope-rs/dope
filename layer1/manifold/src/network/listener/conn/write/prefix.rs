use std::{mem, mem::MaybeUninit};

use dope_net::link::egress::{self, load::Value};
use o3::{
    base::cell::region,
    buffer::write::{Sink, Slice, consume},
};

use crate::network::listener::{conn::write, writer::memory};

mod raw;

/// Callback-scoped proof that a committed prefix belongs to the owned header.
pub struct Prefix<'a, 'id> {
    bytes: Slice<'a>,
    owner: region::Identity<'id>,
}

impl<'a, 'id> Prefix<'a, 'id> {
    pub const CAPACITY: usize = memory::WRITE_BUF_CAP;

    pub(in crate::network::listener::conn::write) fn new(
        bytes: &'a mut [MaybeUninit<u8>; memory::WRITE_BUF_CAP],
        owner: region::Identity<'id>,
    ) -> Self {
        Self {
            bytes: Slice::uninit(bytes),
            owner,
        }
    }

    pub fn writer(&mut self) -> Slice<'_, &mut usize> {
        self.bytes.reborrow()
    }

    pub fn commit<B, R>(
        self,
        body: Option<Value<B>>,
        receipt: R,
    ) -> write::Commit<region::Identity<'id>, egress::load::Value<B>, R> {
        let written = raw::written(self.bytes);
        write::Commit::new(self.owner, written, body, receipt)
    }
}

impl Sink for Prefix<'_, '_> {
    type Error = consume::Error;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        self.bytes.write_byte(byte)
    }

    fn write_slice(&mut self, source: &[u8]) -> Result<(), Self::Error> {
        self.bytes.write_slice(source)
    }

    fn write_slices<const N: usize>(&mut self, sources: [&[u8]; N]) -> Result<(), Self::Error> {
        self.bytes.write_slices(sources)
    }
}

const _: () =
    assert!(mem::size_of::<Prefix<'static, 'static>>() == mem::size_of::<Slice<'static>>());
