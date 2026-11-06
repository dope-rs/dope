use std::mem;

use dope_net::link::egress::{self, load::Value};
use o3::{
    base::{cell::region, num::bounded},
    buffer::write::{Array, Sink},
};
use write::error::{self, Build};

use crate::network::listener::{conn::write, writer::memory};

/// Callback-scoped proof that a committed prefix belongs to the owned header.
pub struct Prefix<'a, 'id> {
    bytes: Array<'a, { Prefix::CAPACITY }>,
    owner: region::Identity<'id>,
}

impl<'a, 'id> Prefix<'a, 'id> {
    pub const CAPACITY: usize = memory::WRITE_BUF_CAP;

    pub(in crate::network::listener::conn::write) fn new(
        bytes: &'a mut [u8; memory::WRITE_BUF_CAP],
        owner: region::Identity<'id>,
    ) -> Self {
        Self {
            bytes: Array::new(bytes),
            owner,
        }
    }

    pub fn commit<B, R>(
        self,
        body: Option<egress::Load<B>>,
        receipt: R,
    ) -> Result<write::Commit<region::Identity<'id>, B, R>, error::Build<B, R>> {
        let written = memory::Len::from(self.bytes.finish());
        let prefix = written.transfer();
        let (total, body) = match body {
            None => (prefix.nonzero(), None),
            Some(body) => match body.try_nonempty() {
                Ok(body) => match body.geometry().wire().checked_add(prefix) {
                    Ok(total) => (Some(total), Some(Value::Checked(body))),
                    Err(cause) => {
                        return Err(Build::overflow(cause, body, receipt));
                    }
                },
                Err(rejected) => match egress::Empty::try_from(rejected) {
                    Ok(_) => (prefix.nonzero(), None),
                    Err(rejected) => return Err(Build::rejected(rejected, receipt)),
                },
            },
        };
        Ok(write::Commit::new(
            self.owner, written, total, body, receipt,
        ))
    }
}

impl Sink for Prefix<'_, '_> {
    type Error = bounded::Error;

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

const _: () = assert!(mem::size_of::<Prefix<'static, 'static>>() == mem::size_of::<&mut [u8]>());
