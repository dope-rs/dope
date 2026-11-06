use std::{mem, num::NonZero};

use crate::wire::{frame::ack::ranges, stream, varint};

use crate::wire::frame::decode::error;

/// Retains the unconsumed suffix of one frame's original borrowed input.
pub(super) struct Cursor<'wire> {
    rest: &'wire [u8],
    len: usize,
}

impl<'wire> Cursor<'wire> {
    pub(super) fn new(input: &'wire [u8]) -> Result<(u8, Self), error::Fault> {
        let Some((&kind, rest)) = input.split_first() else {
            return Err(error::Fault::underflow(0, 0, 1));
        };
        Ok((
            kind,
            Self {
                rest,
                len: rest.len(),
            },
        ))
    }

    pub(super) const fn used(&self) -> NonZero<usize> {
        NonZero::<usize>::MIN.saturating_add(self.len - self.rest.len())
    }

    pub(super) const fn rest(&self) -> &'wire [u8] {
        self.rest
    }

    pub(super) fn varint(&mut self) -> Result<varint::VarInt, error::Fault> {
        let value = varint::Decoded::parse(self.rest).map_err(|source| error::Fault::VarInt {
            offset: self.used().get(),
            source: source.into(),
        })?;
        self.rest = value.rest();
        Ok(value.value())
    }

    pub(super) fn skip(&mut self) -> Result<(), error::Fault> {
        let rest = varint::VarInt::skip(self.rest).map_err(|source| error::Fault::VarInt {
            offset: self.used().get(),
            source,
        })?;
        self.rest = rest;
        Ok(())
    }

    pub(super) fn id(&mut self) -> Result<stream::Id, error::Fault> {
        Ok(stream::Id::from_varint(self.varint()?))
    }

    pub(super) fn count(&mut self) -> Result<varint::VarInt, error::Fault> {
        let value = self.varint()?;
        if value.get() > 1u64 << 60 {
            return Err(error::Fault::InvalidStreamCount { value: value.get() });
        }
        Ok(value)
    }

    pub(super) fn data(&mut self, length: varint::VarInt) -> Result<&'wire [u8], error::Fault> {
        self.bytes(length.get() as usize, length.get() as usize)
    }

    pub(super) fn bytes(
        &mut self,
        len: usize,
        required: usize,
    ) -> Result<&'wire [u8], error::Fault> {
        let Some((bytes, rest)) = self.rest.split_at_checked(len) else {
            return Err(error::Fault::Underflow {
                offset: self.used().get(),
                actual: self.rest.len(),
                required,
            });
        };
        self.rest = rest;
        Ok(bytes)
    }

    pub(super) fn array<const N: usize>(&mut self) -> Result<&'wire [u8; N], error::Fault> {
        let Some((bytes, rest)) = self.rest.split_first_chunk::<N>() else {
            return Err(error::Fault::Underflow {
                offset: self.used().get(),
                actual: self.rest.len(),
                required: N,
            });
        };
        self.rest = rest;
        Ok(bytes)
    }

    pub(super) fn tail(&mut self) -> &'wire [u8] {
        mem::take(&mut self.rest)
    }

    pub(super) fn ranges(
        &mut self,
        count: varint::VarInt,
        largest: varint::VarInt,
        first: varint::VarInt,
    ) -> Result<ranges::Validated<'wire>, error::Fault> {
        let offset = self.used().get();
        ranges::validate(&mut self.rest, offset, count, largest, first)
    }
}
