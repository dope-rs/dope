use std::{cell::Cell, num::NonZero};

use dope_sys::io::reactor::complete;

pub struct Event<'a> {
    pub key: Option<NonZero<u8>>,
    pub value: u8,
    pub terminal: bool,
    pub retired: &'a Cell<u8>,
}

impl complete::Event for Event<'_> {
    type Key = NonZero<u8>;

    fn token(&self) -> Option<Self::Key> {
        self.key
    }

    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

impl Drop for Event<'_> {
    fn drop(&mut self) {
        self.retired.set(self.retired.get() + 1);
    }
}
