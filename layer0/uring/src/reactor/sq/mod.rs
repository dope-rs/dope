use std::{mem, num, os::fd};

pub(crate) use borrowed::Borrowed;
use dope_sys::io::reactor::submit::Target;
pub(crate) use entry::Entry;
pub(crate) use flags::Flags;
use o3::base::num::bounded;
pub(crate) use owned::Owned;
pub(crate) use submit::Submit;

mod borrowed;
mod entry;
mod flags;
mod owned;
pub(crate) mod raw;
mod source;
mod submit;

/// Number of submission entries currently pending in the queue.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub(crate) struct Pending(bounded::U32<0, { u32::MAX }>);

impl From<Pending> for bounded::U32<0, { u32::MAX }> {
    fn from(value: Pending) -> Self {
        value.0
    }
}

impl Pending {
    pub(crate) const fn from_raw(raw: u32) -> Self {
        Self(bounded::U32::clamp(raw as usize))
    }

    pub(crate) const fn is_zero(self) -> bool {
        self.0.get() == 0
    }
}

pub(crate) enum Descriptor {
    File(fd::RawFd),
    Fixed(Fixed),
}

impl Descriptor {
    const fn encode(self) -> (i32, bool) {
        match self {
            Self::File(raw) => (raw, false),
            Self::Fixed(slot) => (slot.index() as i32, true),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pending>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Pending>() == std::mem::align_of::<u32>());
};

/// A fixed-file table index proven to fit the kernel's 24-bit field.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Index([u8; 3]);

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Fixed(num::NonZero<u32>);

impl Index {
    pub(crate) const fn from_le(bytes: [u8; 3]) -> Self {
        Self(bytes)
    }

    const fn encoded(self) -> num::NonZero<u32> {
        let bytes = self.0;
        let raw = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        num::NonZero::<u32>::MIN.saturating_add(raw)
    }
}

impl Fixed {
    pub(crate) const fn new(index: Index) -> Self {
        Self(index.encoded())
    }

    pub(crate) const fn index(self) -> u32 {
        self.0.get() - 1
    }

    pub(crate) const fn encode(self) -> u32 {
        self.0.get()
    }
}

impl Target for Fixed {
    const ZERO: Self = Self::new(Index::from_le([0; 3]));

    fn from_le(bytes: [u8; 3]) -> Self {
        Self::new(Index::from_le(bytes))
    }
}

const _: () = {
    assert!(mem::size_of::<Index>() == 3);
    assert!(mem::size_of::<Fixed>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Fixed>>() == mem::size_of::<u32>());
};
