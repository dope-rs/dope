use std::{cell::Cell, net, os::fd::AsFd as _, time};

use dope_core::{
    Backend, Errno,
    dope_sys::io::poll::{self, Poll as _},
};
use dope_runtime::process::{
    failure::bus,
    lifecycle::{Control, Main, Outcome, WaitError},
};

use crate::harness::{
    peer::{Error, FINISH_TIMEOUT, Finish, Spawn},
    scenario::run,
};

pub(super) fn check(control: Option<Control<'_>>) -> Result<(), Error> {
    if let Some(control) = control
        && control.stopped()?
    {
        return Err(Error::Cancelled);
    }
    Ok(())
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
    control: Option<Control<'_>>,
    entry: poll::Entry<'_>,
    start: time::Instant,
    timeout: Option<time::Duration>,
) -> Result<Option<i16>, Error> {
    if let Some(control) = control {
        let mut entries = [poll::Entry::readable(control.as_fd()), entry];
        let ready = wait(&mut entries, start, timeout)?;
        if entries[0].is_ready() {
            return Err(Error::Cancelled);
        }
        Ok(ready.then_some(entries[1].revents()))
    } else {
        let mut entries = [entry];
        Ok(wait(&mut entries, start, timeout)?.then_some(entries[0].revents()))
    }
}

pub(super) fn io<T>(
    control: Option<Control<'_>>,
    entry: poll::Entry<'_>,
    timeout: Option<time::Duration>,
    nonblocking: bool,
    mut run: impl FnMut() -> Result<T, Errno>,
) -> Result<T, Error> {
    if nonblocking {
        check(control)?;
        return run().map_err(Error::Syscall);
    }
    let start = time::Instant::now();
    let mut waiting = false;
    loop {
        if waiting {
            if ready(control, entry, start, timeout)?.is_none() {
                return Err(Error::TimedOut);
            }
        } else {
            check(control)?;
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
    start: time::Instant,
    timeout: Option<time::Duration>,
) -> Result<bool, Errno> {
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
    main: &'scope mut Option<Main>,
    join: Option<run::Join<'scope, Outcome<T, Error>>>,
}

impl<T: Send> Script<'_, T> {
    /// Cancels forgotten views before joining their worker.
    pub(super) fn with<R>(
        addr: net::SocketAddr,
        worker: impl for<'fd> FnOnce(Control<'fd>) -> Result<T, Error> + Send,
        run: impl for<'scope> FnOnce(Script<'scope, T>) -> R,
    ) -> Result<R, Spawn> {
        let (main, owner) = Main::try_pair().map_err(Spawn::Control)?;
        let mut main = Some(main);
        run::scope(
            run::Entry(move || owner.run(|source| worker(source.control()))),
            |join| {
                let result = run(Script {
                    addr,
                    main: &mut main,
                    join: Some(join),
                });
                main.take();
                result
            },
        )
        .map_err(Spawn::Thread)
    }
}

impl<T> Script<'_, T> {
    pub(super) fn stop(self) -> Result<T, Finish> {
        self.complete(true)
    }

    pub(super) fn finish(self) -> Result<T, Finish> {
        self.complete(false)
    }

    pub(super) fn completion(&self) -> &Main {
        let Some(main) = self.main.as_ref() else {
            unreachable!("main")
        };
        main
    }

    fn complete(self, stop: bool) -> Result<T, Finish> {
        let Some(main) = self.main.take() else {
            unreachable!("main")
        };
        let mut main = if stop { main.stop() } else { Ok(main) };
        let wait = main
            .as_ref()
            .ok()
            .and_then(|main| main.wait_done(FINISH_TIMEOUT).err());
        if wait.is_some() && !stop {
            main = main.and_then(Main::stop);
        }
        self.join(main, wait)
    }

    fn join(mut self, main: Result<Main, Errno>, wait: Option<WaitError>) -> Result<T, Finish> {
        let Some(join) = self.join.take() else {
            unreachable!("joined")
        };
        let output = join.finish();
        let stop = main.err();
        match (output, wait, stop) {
            (Outcome::Done(value), None, None) => Ok(value),
            (
                Outcome::Failed {
                    output: Err(error),
                    report: bus::Report::Clean,
                },
                None,
                None,
            ) => Err(Finish::Worker(error)),
            (output, wait, stop) => {
                let (worker, report) = match output {
                    Outcome::Done(_) => (None, bus::Report::Clean),
                    Outcome::Failed { output, report } => (output.err(), report),
                };
                Err(Finish::Lifecycle {
                    wait,
                    stop,
                    worker,
                    report,
                })
            }
        }
    }
}

impl<T> Drop for Script<'_, T> {
    fn drop(&mut self) {
        self.main.take();
    }
}
