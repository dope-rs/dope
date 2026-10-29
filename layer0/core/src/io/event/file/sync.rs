use crate::{
    Errno,
    driver::{route, route::kind},
    io::event::entry::Entry,
};

#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Sync, Result<(), Errno>>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Sync>,
        outcome: Result<(), Errno>,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Sync> {
        self.0.target()
    }

    pub const fn outcome(&self) -> Result<(), Errno> {
        *self.0.value()
    }

    pub const fn into_parts(self) -> (route::Erased<'d, kind::Sync>, Result<(), Errno>) {
        self.0.split()
    }
}
