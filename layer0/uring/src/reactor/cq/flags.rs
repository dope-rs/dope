use std::{mem, num};

use dope_sys::{
    io::reactor::complete::{self, flags},
    memory::buffer,
};
use flags::Flow;
use o3::base::num::bounded;

const BUFFER: u32 = 1 << 0;
const MORE: u32 = 1 << 1;
const SOCK_NONEMPTY: u32 = 1 << 2;
const SHIFT: u32 = 16;
const INDEX: u32 = (u16::MAX as u32) << SHIFT;
const KNOWN: u32 = BUFFER | MORE | SOCK_NONEMPTY | INDEX;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Flags(u32);

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Buffer(u16);

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Meta {
    buffer: Option<Buffer>,
    flow: Flow,
    socket_nonempty: bool,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Invalid {
    flags: Flags,
}

impl Flags {
    pub(crate) const fn new(raw: u32) -> Self {
        Self(raw)
    }
}

impl complete::Flags for Flags {
    type Meta = Meta;
    type Invalid = Invalid;

    fn meta(self) -> Meta {
        let raw = self.0;
        let buffer = if raw & BUFFER == 0 {
            None
        } else {
            Some(Buffer((raw >> SHIFT) as u16))
        };
        Meta {
            buffer,
            flow: if raw & MORE == 0 {
                Flow::Last
            } else {
                Flow::More
            },
            socket_nonempty: raw & SOCK_NONEMPTY != 0,
        }
    }

    fn decode(self) -> Result<Meta, Invalid> {
        if !self.is_valid() {
            return Err(Invalid { flags: self });
        }
        Ok(self.meta())
    }

    fn is_valid(self) -> bool {
        let raw = self.0;
        raw & !KNOWN == 0 && (raw & BUFFER != 0 || raw & INDEX == 0)
    }
}

impl buffer::Index for Buffer {
    const ZERO: Self = Self(0);

    fn advance(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }

    fn offset(self, stride: num::NonZero<u32>) -> bounded::U64<0, { u64::MAX }> {
        bounded::U64::clamp(self.0 as u64 * stride.get() as u64)
    }

    fn is_within(self, capacity: num::NonZero<u16>) -> bool {
        self.0 < capacity.get()
    }
}

impl From<Invalid> for Flags {
    fn from(invalid: Invalid) -> Self {
        invalid.flags
    }
}

impl flags::Meta for Meta {
    type Buffer = Buffer;

    fn buffer(self) -> Option<Buffer> {
        self.buffer
    }

    fn flow(self) -> Flow {
        self.flow
    }
}

impl From<Meta> for Flags {
    fn from(meta: Meta) -> Self {
        let buffer = match meta.buffer {
            Some(buffer) => BUFFER | (u32::from(buffer.0) << SHIFT),
            None => 0,
        };
        let flow = match meta.flow {
            Flow::Last => 0,
            Flow::More => MORE,
        };
        Self(buffer | flow | (u32::from(meta.socket_nonempty) * SOCK_NONEMPTY))
    }
}

const _: () = {
    assert!(mem::size_of::<Flags>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Invalid>() == mem::size_of::<Flags>());
    assert!(mem::size_of::<Buffer>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Flow>() == mem::size_of::<bool>());
    assert!(mem::size_of::<Option<Buffer>>() == mem::size_of::<u32>());
    assert!(
        mem::size_of::<Meta>() == mem::size_of::<Option<Buffer>>() + 2 * mem::size_of::<bool>()
    );
    assert!(mem::align_of::<Meta>() == mem::align_of::<u16>());
};
