use std::{cell::Cell, rc::Weak};

use dope_test::harness::observe::{Error, Trace};

pub struct Recorded<'a> {
    pub value: u8,
    pub trace: Weak<Trace<Self, 0>>,
    pub dropped: &'a Cell<Vec<(u8, Option<Error>)>>,
}

impl Drop for Recorded<'_> {
    fn drop(&mut self) {
        let failure = self.trace.upgrade().and_then(|trace| trace.values().err());
        let mut dropped = self.dropped.take();
        dropped.push((self.value, failure));
        self.dropped.set(dropped);
    }
}
