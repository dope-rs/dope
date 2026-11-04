use crate::{
    Errno,
    driver::{route, route::kind},
    io::event::entry::Entry,
};

/// Exact native stage that rejected one connection establishment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Option(Errno),
    Connect(Errno),
}

/// A connection completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
///
/// Completion authority is consumed exactly once.
///
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Connect, Result<(), Error>>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Connect>,
        outcome: Result<(), Error>,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Connect> {
        self.0.target()
    }

    pub(in crate::io) fn resolve(
        self,
        expected: route::Erased<'d, kind::Connect>,
    ) -> Result<Result<(), Error>, Self> {
        self.0.resolve(expected).map_err(Self)
    }
}
