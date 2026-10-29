use std::mem;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Code(u8);

impl Code {
    pub(crate) const fn new(raw: u8) -> Self {
        Self(raw)
    }

    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }

    pub(crate) const fn write(self, target: &mut u8) {
        *target = self.0;
    }
}

const _: () = assert!(mem::size_of::<Code>() == mem::size_of::<u8>());
