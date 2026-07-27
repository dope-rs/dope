use libc::c_int;

#[derive(Clone, Copy)]
pub struct SocketOption {
    level: c_int,
    name: c_int,
    value: c_int,
}

impl SocketOption {
    pub const fn new(level: c_int, name: c_int, value: c_int) -> Self {
        Self { level, name, value }
    }

    pub(crate) const fn level(self) -> c_int {
        self.level
    }

    pub(crate) const fn name(self) -> c_int {
        self.name
    }

    pub(crate) const fn value(self) -> c_int {
        self.value
    }
}
