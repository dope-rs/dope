use crate::{
    Errno,
    driver::{retained::flight, route, route::kind},
    io::event::entry::Entry,
};

/// A terminal socket-option transaction completion.
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Tuning, Result<u32, Errno>>);

impl<'d> Completion<'d> {
    pub const fn token(&self) -> route::Erased<'d, kind::Tuning> {
        self.0.token()
    }

    pub const fn outcome(&self) -> Result<(), Errno> {
        match *self.0.value() {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(in crate::io) fn resolve(
        self,
        expected: route::Erased<'d, kind::Tuning>,
    ) -> Result<Result<u32, Errno>, Self> {
        self.0.resolve(expected).map_err(Self)
    }

    pub(crate) fn from_value(
        bound: flight::Complete<'_, 'd, kind::Tuning>,
        value: Result<u32, Errno>,
    ) -> Self {
        Self(Entry::new(bound.complete(), value))
    }
}

impl<'q, 'd> From<(flight::Complete<'q, 'd, kind::Tuning>, Result<(), Errno>)> for Completion<'d> {
    fn from(
        (target, outcome): (flight::Complete<'q, 'd, kind::Tuning>, Result<(), Errno>),
    ) -> Self {
        Self::from_value(target, outcome.map(|()| 0))
    }
}
