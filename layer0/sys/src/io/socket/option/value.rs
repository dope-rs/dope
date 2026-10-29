use std::mem;

/// One socket-option value in the kernel ABI's integer domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Value(libc::c_int);

impl Value {
    pub const ZERO: Self = Self(0);

    pub const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i32 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Encodes one boolean option as the ABI's zero/nonzero switch.
    pub const fn from_enabled(enabled: bool) -> Self {
        Self(enabled as libc::c_int)
    }
}

const _: () = {
    assert!(mem::size_of::<Value>() == mem::size_of::<i32>());
};
