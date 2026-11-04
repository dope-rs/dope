use std::{cell::Cell, net, os::fd, time};

use dope_core::{
    Backend, Errno,
    dope_sys::io::{
        fd::Fd as _,
        poll::{self, Poll as _},
    },
    platform::wake,
};
use fd::AsFd as _;
use o3::base::marker::ThreadBound;

use crate::harness::{
    peer::{Error, FINISH_TIMEOUT, Finish, Spawn},
    scenario::run,
};

/// Only this worker owns its sockets and cancellation reader.
#[derive(Debug)]
pub(super) struct Control {
    read: fd::OwnedFd,
    _thread: ThreadBound,
}

impl Control {
    /// The private nonblocking pipe is never written: live means WouldBlock,
    /// and dropping its sole sender leaves persistent EOF for cancellation.
    pub(super) fn check(&self) -> Result<(), Error> {
        match Backend::read::<Errno>(self.read.as_fd(), &mut [0]) {
            Ok(_) => return Err(Error::Cancelled),
            Err(error) if error.is_block() => return Ok(()),
            Err(error) if error.is_intr() => {}
            Err(error) => return Err(error.into()),
        }
        let mut entries = [poll::Entry::readable(self.read.as_fd())];
        if wait(&mut entries, Some(time::Duration::ZERO))? {
            return Err(Error::Cancelled);
        }
        Ok(())
    }
}

pub(super) fn timeout(
    slot: &Cell<Option<time::Duration>>,
    timeout: Option<time::Duration>,
) -> Result<(), Error> {
    if timeout.is_some_and(|timeout| timeout.is_zero()) {
        return Err(Error::ZeroTimeout);
    }
    slot.set(timeout);
    Ok(())
}

pub(super) fn ready(
    control: Option<&Control>,
    entry: poll::Entry<'_>,
    timeout: Option<time::Duration>,
) -> Result<Option<i16>, Error> {
    if let Some(control) = control {
        let mut entries = [poll::Entry::readable(control.read.as_fd()), entry];
        let ready = wait(&mut entries, timeout)?;
        if entries[0].is_ready() {
            return Err(Error::Cancelled);
        }
        Ok(ready.then_some(entries[1].revents()))
    } else {
        let mut entries = [entry];
        Ok(wait(&mut entries, timeout)?.then_some(entries[0].revents()))
    }
}

pub(super) fn io<T>(
    control: Option<&Control>,
    entry: poll::Entry<'_>,
    timeout: Option<time::Duration>,
    nonblocking: bool,
    mut run: impl FnMut() -> Result<T, Errno>,
) -> Result<T, Error> {
    if nonblocking {
        if let Some(control) = control {
            control.check()?;
        }
        return run().map_err(Error::Syscall);
    }
    let start = time::Instant::now();
    let mut waiting = false;
    loop {
        if waiting {
            let left = timeout.map(|limit| limit.saturating_sub(start.elapsed()));
            if ready(control, entry, left)?.is_none() {
                return Err(Error::TimedOut);
            }
        } else if let Some(control) = control {
            control.check()?;
        }
        match run() {
            Err(error) if error.is_intr() || error.is_block() => {
                if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                    return Err(Error::TimedOut);
                }
                waiting = error.is_block();
            }
            result => return result.map_err(Error::Syscall),
        }
    }
}

fn wait<const N: usize>(
    entries: &mut [poll::Entry<'_>; N],
    timeout: Option<time::Duration>,
) -> Result<bool, Errno> {
    let start = time::Instant::now();
    loop {
        let millis = timeout.map_or(-1, |limit| {
            let left = limit.saturating_sub(start.elapsed());
            let whole = left.as_millis();
            let partial = u128::from(!left.subsec_nanos().is_multiple_of(1_000_000));
            (whole + partial).min(i32::MAX as u128) as i32
        });
        match Backend::wait::<_, Errno>(entries, millis) {
            Ok(events) if events.count() != 0 => return Ok(true),
            Ok(_) => {}
            Err(error) if error.is_intr() => {}
            Err(error) => return Err(error),
        }
        if timeout.is_some_and(|limit| start.elapsed() >= limit) {
            return Ok(false);
        }
    }
}

pub(super) struct Script<'scope, T> {
    pub(super) addr: net::SocketAddr,
    done: &'scope fd::OwnedFd,
    cancel: &'scope Cell<Option<wake::Notify>>,
    join: &'scope mut dyn FnMut() -> Result<T, Error>,
}

impl<T: Send> Script<'_, T> {
    /// Retains cancellation outside the view and closes it before Group joins,
    /// including when the callback forgets the view. HUP remains observable.
    pub(super) fn with<R>(
        addr: net::SocketAddr,
        worker: impl FnOnce(&Control) -> Result<T, Error> + Send,
        run: impl for<'scope> FnOnce(Script<'scope, T>) -> R,
    ) -> Result<R, Spawn> {
        let (read, cancel) = wake::Ends::event().map_err(Spawn::Wake)?.split();
        let (done, complete) = wake::Ends::event().map_err(Spawn::Wake)?.split();
        let cancel = Cell::new(Some(cancel));
        run::scope(
            run::Entry(move || {
                let result = worker(&Control {
                    read,
                    _thread: ThreadBound::NEW,
                });
                drop(complete);
                result
            }),
            |join| {
                let result = run(Script {
                    addr,
                    done: &done,
                    cancel: &cancel,
                    join,
                });
                cancel.take();
                result
            },
        )
        .map_err(Spawn::Thread)
    }
}

impl<T> Script<'_, T> {
    pub(super) fn stop(self) -> Result<T, Finish> {
        self.cancel.take();
        self.finish()
    }

    pub(super) fn finish(self) -> Result<T, Finish> {
        let mut entries = [poll::Entry::readable(self.done.as_fd())];
        let ready = wait(&mut entries, Some(FINISH_TIMEOUT));
        if !matches!(ready, Ok(true)) {
            self.cancel.take();
        }
        let output = (self.join)();
        match ready {
            Ok(true) => output.map_err(Finish::Worker),
            Ok(false) => Err(Finish::Timeout),
            Err(error) => Err(Finish::Worker(Error::Syscall(error))),
        }
    }
}

impl<T> Drop for Script<'_, T> {
    fn drop(&mut self) {
        self.cancel.take();
    }
}
