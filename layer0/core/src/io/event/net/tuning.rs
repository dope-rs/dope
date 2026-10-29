use crate::{
    Errno,
    driver::{retained::flight, route, route::kind},
    io::event::entry::Entry,
};

/// A terminal socket-option transaction completion.
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Tuning, Result<(), Errno>>);

impl<'d> Completion<'d> {
    pub const fn token(&self) -> route::Erased<'d, kind::Tuning> {
        self.0.target()
    }

    pub(in crate::io) fn resolve(
        self,
        expected: route::Erased<'d, kind::Tuning>,
    ) -> Result<Result<(), Errno>, Self> {
        self.0.resolve(expected).map_err(Self)
    }
}

impl<'q, 'd> From<(flight::Complete<'q, 'd, kind::Tuning>, Result<(), Errno>)> for Completion<'d> {
    fn from(
        (target, outcome): (flight::Complete<'q, 'd, kind::Tuning>, Result<(), Errno>),
    ) -> Self {
        let target = target.complete();
        Self(Entry::new(target, outcome))
    }
}
