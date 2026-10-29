use crate::{
    Errno,
    driver::{route, route::kind},
    io::{event::entry::Entry, transfer::Len},
};

#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Send, Result<Len, Errno>>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Send>,
        outcome: Result<Len, Errno>,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Send> {
        self.0.target()
    }

    pub const fn outcome(&self) -> Result<Len, Errno> {
        *self.0.value()
    }

    pub const fn into_parts(self) -> (route::Erased<'d, kind::Send>, Result<Len, Errno>) {
        self.0.split()
    }
}
