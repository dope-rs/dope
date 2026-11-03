use std::num;

use o3::buffer::bytes::scope;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Offset(u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Mask(num::NonZero<u8>);

/// Minimum packet payload extent required by one selector branch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Minimum(num::NonZero<u8>);

/// Packet byte interpreted as the selected shape's declared length.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Length(u8);

/// Selects one dense route byte from either of two packet shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Selector {
    branch_at: Offset,
    branch_mask: Mask,
    clear_key: Offset,
    clear_min: Minimum,
    length_at: Offset,
    length: Length,
    set_key: Offset,
    set_min: Minimum,
}

/// An optional fixed prefix whose tags cannot be mistaken for a direct packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrefixPlan {
    extent: Minimum,
    raw_mask: Mask,
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

impl Offset {
    const fn new(offset: u8) -> Self {
        Self(offset)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

impl Mask {
    const fn new<const MASK: u8>() -> Self {
        const {
            assert!(MASK > 0);
        }
        Self(num::NonZero::<u8>::MIN.saturating_add(MASK - 1))
    }

    pub const fn get(self) -> num::NonZero<u8> {
        self.0
    }
}

impl Minimum {
    const fn new<const BYTES: u8>() -> Self {
        const {
            assert!(BYTES > 0);
        }
        Self(num::NonZero::<u8>::MIN.saturating_add(BYTES - 1))
    }

    pub const fn get(self) -> num::NonZero<u8> {
        self.0
    }
}

impl Length {
    pub(crate) const fn new(bytes: u8) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
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
            assert!(BRANCH_AT < CLEAR_MIN);
            assert!(CLEAR_KEY < CLEAR_MIN);
            assert!(BRANCH_AT < SET_MIN);
            assert!(LENGTH_AT < SET_MIN);
            assert!(SET_KEY < SET_MIN);
            assert!(CLEAR_MIN <= SET_MIN);
        }
        Self {
            branch_at: Offset::new(BRANCH_AT),
            branch_mask: Mask::new::<BRANCH_MASK>(),
            clear_key: Offset::new(CLEAR_KEY),
            clear_min: Minimum::new::<CLEAR_MIN>(),
            length_at: Offset::new(LENGTH_AT),
            length: Length::new(LENGTH),
            set_key: Offset::new(SET_KEY),
            set_min: Minimum::new::<SET_MIN>(),
        }
    }

    pub const fn branch_at(self) -> Offset {
        self.branch_at
    }

    pub const fn branch_mask(self) -> Mask {
        self.branch_mask
    }

    pub const fn clear_key(self) -> Offset {
        self.clear_key
    }

    pub const fn clear_min(self) -> Minimum {
        self.clear_min
    }

    pub const fn length_at(self) -> Offset {
        self.length_at
    }

    pub const fn length(self) -> Length {
        self.length
    }

    pub const fn set_key(self) -> Offset {
        self.set_key
    }

    pub const fn set_min(self) -> Minimum {
        self.set_min
    }
}

impl PrefixPlan {
    pub const fn fixed<const BYTES: u8, const RAW_MASK: u8, const FIRST: u8, const SECOND: u8>()
    -> Self {
        const {
            assert!(FIRST != SECOND);
            assert!(FIRST & RAW_MASK == 0);
            assert!(SECOND & RAW_MASK == 0);
        }
        Self {
            extent: Minimum::new::<BYTES>(),
            raw_mask: Mask::new::<RAW_MASK>(),
            tags: [FIRST, SECOND],
        }
    }

    pub const fn extent(self) -> num::NonZero<u8> {
        self.extent.get()
    }

    pub const fn raw_mask(self) -> Mask {
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
        let raw_mask = self.raw_mask.get().get();
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
