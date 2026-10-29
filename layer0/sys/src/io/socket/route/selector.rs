use std::num;

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

const _: () = {
    assert!(std::mem::size_of::<Selector>() == 8);
    assert!(std::mem::align_of::<Selector>() == 1);
};
