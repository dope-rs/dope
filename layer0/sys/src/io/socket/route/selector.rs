use std::num;

use o3::buffer::bytes::scope;

/// Selects one dense route byte from either of two packet shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Selector {
    branch_at: u8,
    branch_mask: num::NonZero<u8>,
    clear_key: u8,
    clear_min: num::NonZero<u8>,
    length_at: u8,
    length: u8,
    set_key: u8,
    set_min: num::NonZero<u8>,
}

/// An optional fixed prefix whose tags cannot be mistaken for a direct packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrefixPlan {
    extent: num::NonZero<u8>,
    raw_mask: num::NonZero<u8>,
    tags: [u8; 2],
}

/// A borrowed prefix and the remaining range bound to its original datagram.
#[derive(Debug)]
pub enum Frame<'a, 'id> {
    Direct,
    Prefixed {
        prefix: &'a [u8],
        remaining: scope::Range<'id>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrefixError {
    Truncated {
        actual: usize,
        required: num::NonZero<usize>,
    },
    Unknown(u8),
    Nested(u8),
    Inner(u8),
}

impl Selector {
    pub const fn fixed<
        const BRANCH_AT: u8,
        const BRANCH_MASK: u8,
        const CLEAR_KEY: u8,
        const CLEAR_MIN: u8,
        const LENGTH_AT: u8,
        const LENGTH: u8,
        const SET_KEY: u8,
        const SET_MIN: u8,
    >() -> Self {
        const {
            assert!(BRANCH_MASK > 0);
            assert!(BRANCH_AT < CLEAR_MIN);
            assert!(CLEAR_KEY < CLEAR_MIN);
            assert!(BRANCH_AT < SET_MIN);
            assert!(LENGTH_AT < SET_MIN);
            assert!(SET_KEY < SET_MIN);
            assert!(CLEAR_MIN <= SET_MIN);
        }
        Self {
            branch_at: BRANCH_AT,
            branch_mask: num::NonZero::<u8>::MIN.saturating_add(BRANCH_MASK - 1),
            clear_key: CLEAR_KEY,
            clear_min: num::NonZero::<u8>::MIN.saturating_add(CLEAR_MIN - 1),
            length_at: LENGTH_AT,
            length: LENGTH,
            set_key: SET_KEY,
            set_min: num::NonZero::<u8>::MIN.saturating_add(SET_MIN - 1),
        }
    }

    pub const fn branch_at(self) -> u8 {
        self.branch_at
    }

    pub const fn branch_mask(self) -> num::NonZero<u8> {
        self.branch_mask
    }

    pub const fn clear_key(self) -> u8 {
        self.clear_key
    }

    pub const fn clear_min(self) -> num::NonZero<u8> {
        self.clear_min
    }

    pub const fn length_at(self) -> u8 {
        self.length_at
    }

    pub const fn length(self) -> u8 {
        self.length
    }

    pub const fn set_key(self) -> u8 {
        self.set_key
    }

    pub const fn set_min(self) -> num::NonZero<u8> {
        self.set_min
    }
}

impl PrefixPlan {
    pub const fn fixed<const BYTES: u8, const RAW_MASK: u8, const FIRST: u8, const SECOND: u8>()
    -> Self {
        const {
            assert!(BYTES > 0);
            assert!(RAW_MASK > 0);
            assert!(FIRST != SECOND);
            assert!(FIRST & RAW_MASK == 0);
            assert!(SECOND & RAW_MASK == 0);
        }
        Self {
            extent: num::NonZero::<u8>::MIN.saturating_add(BYTES - 1),
            raw_mask: num::NonZero::<u8>::MIN.saturating_add(RAW_MASK - 1),
            tags: [FIRST, SECOND],
        }
    }

    pub const fn extent(self) -> num::NonZero<u8> {
        self.extent
    }

    pub const fn raw_mask(self) -> num::NonZero<u8> {
        self.raw_mask
    }

    pub const fn tags(self) -> [u8; 2] {
        self.tags
    }

    pub fn try_parse<'a, 'id>(
        self,
        source: scope::View<'a, 'id>,
    ) -> Result<Frame<'a, 'id>, PrefixError> {
        let packet = source.as_slice();
        let first = *packet.first().ok_or(PrefixError::Truncated {
            actual: 0,
            required: num::NonZero::<usize>::MIN,
        })?;
        let raw_mask = self.raw_mask.get();
        if first & raw_mask != 0 {
            return Ok(Frame::Direct);
        }
        if !self.tags.contains(&first) {
            return Err(PrefixError::Unknown(first));
        }
        let truncated = PrefixError::Truncated {
            actual: packet.len(),
            required: num::NonZero::<usize>::from(self.extent()).saturating_add(1),
        };
        let (prefix, packet, remaining) = source
            .try_split_at(self.extent().into())
            .map_err(|_| truncated)?;
        let first = *packet.first().ok_or(truncated)?;
        if self.tags.contains(&first) {
            return Err(PrefixError::Nested(first));
        }
        if first & raw_mask == 0 {
            return Err(PrefixError::Inner(first));
        }
        Ok(Frame::Prefixed { prefix, remaining })
    }
}

const _: () = {
    assert!(std::mem::size_of::<Selector>() == 8);
    assert!(std::mem::align_of::<Selector>() == 1);
    assert!(std::mem::size_of::<PrefixPlan>() == 4);
    assert!(std::mem::align_of::<PrefixPlan>() == 1);
};
