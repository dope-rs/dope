use std::mem;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Kind(u16);

impl Kind {
    pub(crate) const A: Self = Self(1);
    pub(crate) const CNAME: Self = Self(5);
    pub(crate) const AAAA: Self = Self(28);
    pub(crate) const OPT: Self = Self(41);

    pub(crate) const fn from_wire(value: u16) -> Self {
        Self(value)
    }

    pub(crate) const fn wire(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Class(u16);

impl Class {
    pub(crate) const IN: Self = Self(1);

    pub(crate) const fn from_wire(value: u16) -> Self {
        Self(value)
    }

    pub(crate) const fn wire(self) -> u16 {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Kind>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Class>() == mem::size_of::<u16>());
};
