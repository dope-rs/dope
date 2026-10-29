use std::os::fd;

use crate::{
    driver::{route, route::kind},
    io::event::entry::Entry,
};

pub enum Outcome {
    Opened(fd::OwnedFd),
    Failed(crate::Errno),
}

/// An open completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Open, Outcome>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Open>,
        outcome: Outcome,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Open> {
        self.0.target()
    }

    pub const fn outcome(&self) -> &Outcome {
        self.0.value()
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Open>, Outcome) {
        self.0.into_parts()
    }
}
