use core::array;

/// A fixed-batch position proven to be below its `N` capacity.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Index<const N: usize>(u16);

impl<const N: usize> Index<N> {
    pub const fn get(self) -> usize {
        self.0 as usize
    }

    pub(crate) fn map<T>(mut make: impl FnMut(Self) -> T) -> [T; N] {
        const { assert!(N > 0 && N <= u16::MAX as usize) };
        array::from_fn(|raw| make(Self(raw as u16)))
    }
}

const _: () = assert!(std::mem::size_of::<Index<1>>() == std::mem::size_of::<u16>());
