use std::mem;

use o3::base::marker::ThreadBound;

use crate::process::{
    failure::bus::{Event, Fault},
    runtime::bus,
};

/// Single-use proof that one worker still holds its application gate closed.
#[must_use]
#[repr(transparent)]
pub struct Closed<'run> {
    tx: bus::Tx<'run>,
    thread: ThreadBound,
}

/// Single-use proof that one worker has opened its application gate.
#[must_use]
#[repr(transparent)]
pub struct Open<'run> {
    tx: bus::Tx<'run>,
    thread: ThreadBound,
}

impl<'run> Closed<'run> {
    pub(in crate::process) const fn new(tx: bus::Tx<'run>) -> Self {
        Self {
            tx,
            thread: ThreadBound::NEW,
        }
    }

    pub fn open(self) -> Result<Open<'run>, Fault> {
        let tx = self.tx;
        tx.send(Event::Ready)?;
        Ok(Open::new(tx))
    }
}

impl<'run> Open<'run> {
    const fn new(tx: bus::Tx<'run>) -> Self {
        Self {
            tx,
            thread: ThreadBound::NEW,
        }
    }

    pub fn finish(self) -> Result<(), Fault> {
        self.tx.send(Event::Drain)
    }
}

const _: () = {
    assert!(mem::size_of::<Closed<'static>>() == mem::size_of::<bus::Tx<'static>>());
    assert!(mem::align_of::<Closed<'static>>() == mem::align_of::<bus::Tx<'static>>());
    assert!(mem::size_of::<Open<'static>>() == mem::size_of::<bus::Tx<'static>>());
    assert!(mem::align_of::<Open<'static>>() == mem::align_of::<bus::Tx<'static>>());
};
