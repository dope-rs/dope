/// One creation mode for `open(2)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Mode(libc::mode_t);

impl Mode {
    pub const fn new(bits: u32) -> Self {
        Self(bits as libc::mode_t)
    }

    pub const fn raw(self) -> libc::mode_t {
        self.0
    }
}
