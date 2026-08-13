use std::cell;

use dope_core::driver::route::{self, table};

#[repr(transparent)]
pub(super) struct Slot<Tag: route::Tag>(cell::Cell<Option<(table::Parts<Tag>, bool)>>);

impl<Tag: route::Tag> Slot<Tag> {
    pub(super) const fn empty() -> Self {
        Self(cell::Cell::new(None))
    }

    pub(super) fn take(&self) -> Option<(table::Parts<Tag>, bool)> {
        self.0.take()
    }

    pub(super) fn store(&self, parts: table::Parts<Tag>, deferred: bool) {
        self.0.set(Some((parts, deferred)));
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.get().is_none()
    }

    pub(super) fn is_deferred(&self) -> bool {
        self.0.get().is_some_and(|(_, deferred)| deferred)
    }
}
