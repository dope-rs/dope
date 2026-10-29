use o3::buffer::bytes;

use crate::io::transfer;

/// Borrowed byte prefix whose length fits every backend completion ABI.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct View<'a>(bytes::Prefix<'a, { transfer::MAX_BYTES }>);

impl<'a> View<'a> {
    #[must_use]
    pub fn prefix(bytes: &'a [u8]) -> Self {
        Self(bytes::Prefix::new(bytes))
    }

    #[must_use]
    pub fn into_parts(self) -> (&'a [u8], transfer::Len) {
        let bytes = self.0.into_slice();
        (bytes, self.len())
    }

    pub(crate) const fn into_prefix(self) -> bytes::Prefix<'a, { transfer::MAX_BYTES }> {
        self.0
    }

    #[must_use]
    pub const fn len(&self) -> transfer::Len {
        transfer::Len::from_bound(self.0.len())
    }

    #[must_use]
    pub const fn as_slice(&self) -> &'a [u8] {
        self.0.as_slice()
    }
}

const _: () = assert!(std::mem::size_of::<View<'static>>() == std::mem::size_of::<&'static [u8]>());
