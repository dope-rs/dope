use std::mem;

use crate::io;

/// One accepted-socket completion and the lifetime of its kernel source.
pub struct Completion<'d> {
    more: bool,
    event: io::AcceptEvent<'d>,
}

const _: () = assert!(
    mem::size_of::<Completion<'static>>() == mem::size_of::<(bool, io::AcceptEvent<'static>)>()
);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(more: bool, event: io::AcceptEvent<'d>) -> Self {
        Self { more, event }
    }

    pub const fn more(&self) -> bool {
        self.more
    }

    pub const fn event(&self) -> &io::AcceptEvent<'d> {
        &self.event
    }

    pub fn into_parts(self) -> (bool, io::AcceptEvent<'d>) {
        (self.more, self.event)
    }
}
