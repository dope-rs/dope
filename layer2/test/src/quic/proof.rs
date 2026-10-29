use std::mem;

/// Consuming proof that an oracle observed complete delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    delivered: usize,
    payload_bytes: usize,
}

impl Completion {
    pub(super) const fn new(done: usize, bytes: usize) -> Self {
        Self {
            delivered: done,
            payload_bytes: bytes,
        }
    }

    pub const fn delivered(self) -> usize {
        self.delivered
    }

    pub const fn bytes(self) -> usize {
        self.payload_bytes
    }

    pub fn try_merge(self, other: Self) -> Result<Self, Overflow> {
        let done = self
            .delivered
            .checked_add(other.delivered)
            .ok_or(Overflow::Delivered {
                current: self.delivered,
                added: other.delivered,
            })?;
        let bytes = self
            .payload_bytes
            .checked_add(other.payload_bytes)
            .ok_or(Overflow::Bytes {
                current: self.payload_bytes,
                added: other.payload_bytes,
            })?;
        Ok(Self {
            delivered: done,
            payload_bytes: bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    Delivered { current: usize, added: usize },
    Bytes { current: usize, added: usize },
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Completion>() == 2 * word);
    assert!(mem::size_of::<Overflow>() == 3 * word);
    assert!(mem::size_of::<Result<Completion, Overflow>>() == 3 * word);
};

/// A consumed oracle that had not yet observed its complete scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Incomplete {
    pub expected: usize,
    pub delivered: usize,
}
