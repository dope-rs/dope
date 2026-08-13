use std::{io, marker, os::fd};

use dope_core::driver::settings;

use crate::executor;

mod wakes;

pub(crate) use wakes::{Ends, Notify, Wait};

#[must_use]
pub struct Pair {
    source: Source,
    trigger: Trigger,
}

#[must_use]
pub struct Source {
    pub(crate) event: fd::OwnedFd,
    _guard: Notify,
}

#[must_use]
pub struct Trigger(Notify);

#[must_use]
pub struct Requested<Q = Source> {
    _source: marker::PhantomData<fn(Q) -> Q>,
    _thread: o3::ThreadBound,
}

const _: () = assert!(std::mem::size_of::<Requested>() == 0);
const _: () = assert!(std::mem::size_of::<Requested<crate::process::Shutdown>>() == 0);

impl Pair {
    pub fn new() -> io::Result<Self> {
        let (event, notify) = Ends::event()?.split();
        let (guard, notify) = notify.fork()?;
        Ok(Self {
            source: Source {
                event,
                _guard: guard,
            },
            trigger: Trigger(notify),
        })
    }

    pub fn split(self) -> (Source, Trigger) {
        (self.source, self.trigger)
    }
}

impl executor::Factory for Source {
    type Shutdown = Source;

    fn executor(
        self,
        config: settings::Config,
    ) -> io::Result<executor::Executor<(), Self::Shutdown>> {
        executor::Executor::new(config)?.with_shutdown(self)
    }
}

impl Trigger {
    pub fn fire(self) -> io::Result<()> {
        self.0.notify()
    }
}

impl<Q> Requested<Q> {
    pub(crate) const fn new() -> Self {
        use o3::ThreadBound;

        Self {
            _source: marker::PhantomData,
            _thread: ThreadBound::NEW,
        }
    }
}
