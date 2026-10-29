use std::{cell, mem};

use dope::core::driver::schedule::ready;

use crate::reply::{
    LaneIndex,
    arena::state::EntryKey,
    retain::{grant::payload, pool},
};

#[derive(Clone, Copy)]
pub(super) struct Wait {
    pub(super) key: EntryKey,
    pub(super) request: payload::Requested,
}

pub(super) struct Wake<'region> {
    pub(super) target: cell::Cell<Option<ready::Target<'region>>>,
    pub(super) waiting: cell::Cell<Option<Wait>>,
    pub(super) links: [pool::raw::Link<LaneIndex>; 3],
}

impl Wake<'_> {
    pub(super) fn new() -> Self {
        Self {
            target: cell::Cell::new(None),
            waiting: cell::Cell::new(None),
            links: [const { pool::raw::Link::new() }; 3],
        }
    }

    pub(super) fn cancel(&self, key: EntryKey) {
        if self.waiting.get().is_some_and(|wait| wait.key == key) {
            self.waiting.set(None);
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Wait>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Wait>>() == mem::size_of::<Wait>());
};
