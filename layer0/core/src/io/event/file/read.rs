use crate::{
    Errno,
    driver::{route, route::kind},
    io::{event::entry::Entry, transfer::Len},
};

/// Exact result of one file read into its retained owner's output.
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Read, Result<Len, Errno>>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Read>,
        outcome: Result<Len, Errno>,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Read> {
        self.0.target()
    }

    pub const fn outcome(&self) -> Result<Len, Errno> {
        *self.0.value()
    }

    pub const fn into_parts(self) -> (route::Erased<'d, kind::Read>, Result<Len, Errno>) {
        self.0.split()
    }
}
