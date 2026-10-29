use crate::harness::scenario::process::{Error, Resource};

#[derive(Debug)]
pub(super) struct State<R: Resource> {
    limit: R::Limits,
    handler: R::Handler,
}

impl<R: Resource> State<R> {
    pub(super) fn try_zero() -> Result<Self, Error> {
        let limit = R::limits().map_err(Error::Limit)?;
        let handler = R::ignore().map_err(Error::Signal)?;
        if let Err(limit) = R::soft(&limit, 0) {
            return Err(match R::signal(handler) {
                Ok(()) => Error::Limit(limit),
                Err(signal) => Error::Both { limit, signal },
            });
        }
        Ok(Self { limit, handler })
    }

    pub(super) fn try_reset(self) -> Result<(), Error> {
        let limit = R::restore(&self.limit);
        let signal = R::signal(self.handler);
        match (limit, signal) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(Error::Limit(error)),
            (Ok(()), Err(error)) => Err(Error::Signal(error)),
            (Err(limit), Err(signal)) => Err(Error::Both { limit, signal }),
        }
    }
}
