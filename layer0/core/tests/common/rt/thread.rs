use std::{cell::Cell, num::NonZero};

use dope_core::platform::thread;

pub const STACK: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(8 * 1024 * 1024 - 1);

pub struct Write<'a> {
    pub target: &'a mut u64,
    pub value: u64,
}

impl thread::Task for Write<'_> {
    type Output = u64;

    fn run(self) -> u64 {
        *self.target = self.value;
        self.value
    }
}

pub struct Owned<'a>(pub &'a mut u32);

impl thread::Task for Owned<'_> {
    type Output = Self;

    fn run(self) -> Self {
        self
    }
}

impl Drop for Owned<'_> {
    fn drop(&mut self) {
        *self.0 += 1;
    }
}

pub struct Order<'a> {
    pub order: &'a Cell<u32>,
    pub value: u32,
}

impl Drop for Order<'_> {
    fn drop(&mut self) {
        self.order.set(self.order.get() * 10 + self.value);
    }
}
