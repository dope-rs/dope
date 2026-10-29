use std::{mem, ops};

use dope_sys::io::socket::{
    option,
    option::{Kind, Value, tuning::device},
};
use o3::depot::capacity;

use crate::io::socket::option::{MAX_STREAM_OPTIONS, Set};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::io::socket) struct Mask(u8);

#[derive(Clone, Copy)]
pub(crate) struct NonEmpty<'a> {
    options: &'a Set,
    bits: Mask,
    next: Kind,
    count: capacity::Limit<MAX_STREAM_OPTIONS>,
}

impl Mask {
    pub(super) const EMPTY: Self = Self(0);

    pub(super) fn insert(&mut self, kind: Kind) {
        self.0 |= kind.bit();
    }

    pub(super) fn contains(self, kind: Kind) -> bool {
        self.0 & kind.bit() != 0
    }

    fn first(self) -> Option<Kind> {
        if self.contains(Kind::NoDelay) {
            Some(Kind::NoDelay)
        } else if self.contains(Kind::KeepAlive) {
            Some(Kind::KeepAlive)
        } else if self.contains(Kind::RecvBuffer) {
            Some(Kind::RecvBuffer)
        } else if self.contains(Kind::SendBuffer) {
            Some(Kind::SendBuffer)
        } else if self.contains(Kind::KeepAliveIdle) {
            Some(Kind::KeepAliveIdle)
        } else if self.contains(Kind::KeepAliveInterval) {
            Some(Kind::KeepAliveInterval)
        } else if self.contains(Kind::KeepAliveRetries) {
            Some(Kind::KeepAliveRetries)
        } else {
            None
        }
    }

    fn remove(&mut self, kind: Kind) {
        self.0 &= !kind.bit();
    }
}

impl<'a> NonEmpty<'a> {
    pub(super) fn new(options: &'a Set, bits: Mask) -> Option<Self> {
        let next = bits.first()?;
        let count = capacity::Limit::new(bits.0.count_ones() as usize)?;
        Some(Self {
            options,
            bits,
            next,
            count,
        })
    }
}

impl<'a> device::Options<MAX_STREAM_OPTIONS> for NonEmpty<'a> {
    type Cursor = Cursor<'a>;

    fn split(self) -> (capacity::Limit<MAX_STREAM_OPTIONS>, Self::Cursor) {
        (self.count, self.into())
    }
}

impl<'a> From<NonEmpty<'a>> for Cursor<'a> {
    fn from(options: NonEmpty<'a>) -> Self {
        Self {
            options: options.options,
            bits: options.bits,
            next: options.next,
        }
    }
}

pub(crate) struct Cursor<'a> {
    options: &'a Set,
    bits: Mask,
    next: Kind,
}

impl option::Cursor for Cursor<'_> {
    type Step = Step;

    fn pop(&mut self) -> Step {
        let kind = self.next;
        self.bits.remove(kind);
        let step = match self.bits.first() {
            Some(next) => {
                self.next = next;
                ops::ControlFlow::Continue(kind)
            }
            None => ops::ControlFlow::Break(kind),
        };
        Step {
            kind: step,
            value: self.options.value(kind),
        }
    }
}

pub(crate) struct Step {
    kind: ops::ControlFlow<Kind, Kind>,
    value: Value,
}

impl option::Step for Step {
    fn kind(&self) -> ops::ControlFlow<Kind, Kind> {
        self.kind
    }

    fn value(&self) -> &Value {
        &self.value
    }
}

const _: () = {
    assert!(mem::size_of::<Mask>() == mem::size_of::<u8>());
    assert!(mem::size_of::<NonEmpty<'_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Cursor<'_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Cursor<'_>>>() == mem::size_of::<Cursor<'_>>());
    assert!(mem::size_of::<Step>() == 2 * mem::size_of::<Value>());
    assert!(mem::size_of::<Option<Step>>() == mem::size_of::<Step>());
};
