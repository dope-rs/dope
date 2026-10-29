use std::{convert, num};

/// Segmentation capability and its borrowed ancillary storage.
pub trait Mode: Sized {
    type Capability;

    fn acquire() -> Option<Self::Capability>;
    fn control(cap: Self::Capability, size: num::NonZero<u16>) -> Self;
    fn release(self) -> (Self::Capability, num::NonZero<u16>);
    fn bytes(&mut self) -> &[u8];
}

impl Mode for convert::Infallible {
    type Capability = Self;

    fn acquire() -> Option<Self::Capability> {
        None
    }

    fn control(cap: Self::Capability, _: num::NonZero<u16>) -> Self {
        match cap {}
    }

    fn release(self) -> (Self::Capability, num::NonZero<u16>) {
        match self {}
    }

    fn bytes(&mut self) -> &[u8] {
        match *self {}
    }
}
