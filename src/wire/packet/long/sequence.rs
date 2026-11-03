use std::{mem, num::NonZero};

use dope::manifold::network::datagram::packet;
use o3::buffer::storage::provided;
use o3::buffer::write::consume;

use crate::wire::packet::{decode, long};

/// Takes one checked packet while the original owner retains the remaining bytes.
pub(crate) trait Sequence {
    type First: long::Buffer;

    fn first(self) -> Result<long::Parsed<Self::First>, decode::Error>;

    fn take(self, len: NonZero<usize>) -> Result<Self::First, decode::Error>;
}

impl<'a, 'turn, 'd, T: provided::Mutable<'d>> Sequence for &'a mut packet::Packet<'turn, 'd, T> {
    type First = packet::Head<'a, 'turn, 'd, T::Prefix<'a>>;

    fn first(self) -> Result<long::Parsed<Self::First>, decode::Error> {
        let read = long::Read::first(self.as_ref())?;
        self.take(read.len).map(|packet| read.bind(packet))
    }

    fn take(self, len: NonZero<usize>) -> Result<Self::First, decode::Error> {
        self.try_take(len).map_err(decode::Error::Packet)
    }
}

impl<'a> Sequence for &'a mut &mut [u8] {
    type First = &'a mut [u8];

    fn first(self) -> Result<long::Parsed<Self::First>, decode::Error> {
        let read = long::Read::first(self)?;
        self.take(read.len).map(|packet| read.bind(packet))
    }

    fn take(self, len: NonZero<usize>) -> Result<Self::First, decode::Error> {
        consume::Error::check(len.get(), self.len()).map_err(decode::Error::Packet)?;
        let (head, tail) = mem::take(self).split_at_mut(len.get());
        *self = tail;
        Ok(head)
    }
}
