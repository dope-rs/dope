use std::{
    cell, marker, mem,
    os::fd::{self, AsFd as _},
    time,
};

use dope_core::{
    Backend, Errno,
    dope_sys::io::{
        fd::Fd as _,
        poll::{self, Poll as _},
        socket::{How, Socket},
    },
    driver::{life::creation, settings},
};
use o3::base::marker::ThreadBound;

use crate::{
    executor::{self, create},
    process::{self, failure::bus},
    shutdown,
};

mod native;
pub use native::{Entry, Join, Start, Task, scope};

#[derive(Debug)]
pub enum WaitError {
    Poll(Errno),
    Read(bus::Read),
    Event(bus::Event),
    Timeout {
        timeout: time::Duration,
        elapsed: time::Duration,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct Control<'a> {
    fd: fd::BorrowedFd<'a>,
    _thread: ThreadBound,
}

pub(crate) fn pair() -> Result<(Main, fd::OwnedFd), bus::Create> {
    let (left, right) = Backend::pair()?;
    Ok((Main(left, marker::PhantomData), right))
}

#[derive(Debug)]
pub struct Main(fd::OwnedFd, marker::PhantomData<cell::Cell<()>>);

pub struct Worker {
    socket: fd::OwnedFd,
    failure: cell::Cell<Option<bus::Fault>>,
    phase: cell::Cell<Phase>,
}

#[derive(Debug)]
pub enum Outcome<T, E> {
    Done(T),
    Failed {
        output: Result<T, E>,
        report: bus::Report,
    },
}

#[derive(Clone, Copy)]
enum Phase {
    New,
    Installed,
    Failed,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::process) struct Tx<'a>(&'a Worker, marker::PhantomData<fn(&'a ()) -> &'a ()>);

/// Selects the worker event sequence from its `Plain` or `Gated` entry type.
pub(in crate::process) trait Mode {
    /// Whether the worker publishes application ready and drained events.
    const GATED: bool;
}

/// Retains whether shutdown was requested before application readiness.
#[derive(Clone, Copy)]
pub(in crate::process) enum Readiness {
    Required,
    Cancelled,
}

impl Main {
    pub fn try_pair() -> Result<(Self, Worker), bus::Create> {
        let (main, right) = pair()?;
        Ok((
            main,
            Worker {
                socket: right,
                failure: cell::Cell::new(None),
                phase: cell::Cell::new(Phase::New),
            },
        ))
    }

    pub(in crate::process) fn recv(&self) -> Result<bus::Event, bus::Read> {
        let mut byte = [0];
        loop {
            match Backend::recv::<0, Errno>(self.0.as_fd(), &mut byte) {
                Ok(bytes) => {
                    return match bytes.first() {
                        Some(value) => bus::Event::decode(*value).map_err(bus::Read::Event),
                        None => Err(bus::Read::Closed),
                    };
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(bus::Read::Syscall(error)),
            }
        }
    }

    pub(in crate::process) fn handoff<M: Mode>(&self, readiness: Readiness) -> Option<bus::Read> {
        use bus::Event;

        let mut failure = None;
        let mut previous = None;
        loop {
            match self.recv() {
                Err(bus::Read::Closed) => return failure,
                Ok(event) => {
                    if failure.is_none() {
                        let valid = match (previous, event) {
                            (None, _) if M::GATED => true,
                            (None, Event::Installed | Event::Done | Event::Failed) => true,
                            (None, Event::Ready | Event::Drain) => false,
                            (Some(Event::Installed), Event::Ready) => M::GATED,
                            (Some(Event::Installed), Event::Done) => {
                                !M::GATED || matches!(readiness, Readiness::Cancelled)
                            }
                            (Some(Event::Installed), Event::Failed) => true,
                            (Some(Event::Installed), Event::Installed | Event::Drain) => false,
                            (Some(Event::Ready), Event::Drain | Event::Failed) => M::GATED,
                            (Some(Event::Ready), Event::Installed | Event::Ready | Event::Done) => {
                                false
                            }
                            (Some(Event::Drain), Event::Done | Event::Failed) => M::GATED,
                            (
                                Some(Event::Drain),
                                Event::Installed | Event::Ready | Event::Drain,
                            ) => false,
                            (Some(Event::Done | Event::Failed), _) => false,
                        };
                        if !valid {
                            failure = Some(bus::Read::Sequence(bus::Sequence {
                                gated: M::GATED,
                                previous,
                                event,
                            }));
                        }
                    }
                    previous = Some(event);
                }
                Err(source) => {
                    let terminal = matches!(&source, bus::Read::Syscall(_));
                    if failure.is_none() {
                        failure = Some(source);
                    }
                    if terminal {
                        return failure;
                    }
                }
            }
        }
    }

    pub(crate) fn duplicate(&self) -> Result<Self, Errno> {
        Backend::duplicate(self.0.as_fd()).map(|fd| Self(fd, marker::PhantomData))
    }

    pub fn request(&self) -> Result<(), Errno> {
        shutdown(self.0.as_fd()).map_or(Ok(()), Err)
    }

    pub fn stop(self) -> Result<Self, Errno> {
        self.request()?;
        Ok(self)
    }

    pub fn wait_ready(&self, timeout: time::Duration) -> Result<(), WaitError> {
        wait(self.0.as_fd(), time::Instant::now(), timeout)?;
        match self.recv().map_err(WaitError::Read)? {
            bus::Event::Installed => Ok(()),
            event => Err(WaitError::Event(event)),
        }
    }

    pub fn wait_done(&self, timeout: time::Duration) -> Result<(), WaitError> {
        wait(self.0.as_fd(), time::Instant::now(), timeout)?;
        match self.recv().map_err(WaitError::Read)? {
            bus::Event::Done | bus::Event::Failed => Ok(()),
            event => Err(WaitError::Event(event)),
        }
    }

    pub fn run<R>(
        self,
        timeout: time::Duration,
        run: impl FnOnce() -> R,
    ) -> (Result<R, WaitError>, Result<Self, Errno>) {
        let output = self.wait_ready(timeout).map(|()| run());
        (output, self.stop())
    }
}

impl fd::AsFd for Main {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl executor::Startup for &Main {
    const INTEREST: dope_core::dope_sys::engine::register::Interest =
        dope_core::dope_sys::engine::register::Interest::Closed;

    fn installed(&self) -> Result<(), executor::InstallError> {
        Ok(())
    }
}

/// Observes terminal shutdown without consuming lifecycle records.
impl executor::Build for &Main {
    type Shutdown = Self;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<executor::Executor<(), Self::Shutdown>, create::Error<'_>> {
        executor::Executor::new(attempt, config)?
            .shutdown(self)
            .map_err(create::Error::from)
    }
}

fn wait(
    fd: fd::BorrowedFd<'_>,
    started: time::Instant,
    timeout: time::Duration,
) -> Result<(), WaitError> {
    loop {
        let left = timeout.saturating_sub(started.elapsed());
        let millis = left.as_millis() + u128::from(!left.subsec_nanos().is_multiple_of(1_000_000));
        let mut entries = [poll::Entry::readable(fd)];
        match Backend::wait::<_, Errno>(&mut entries, millis.min(i32::MAX as u128) as i32) {
            Ok(events) if events.count() != 0 => return Ok(()),
            Ok(_) => {}
            Err(error) if error.is_intr() => {}
            Err(error) => return Err(WaitError::Poll(error)),
        }
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return Err(WaitError::Timeout { timeout, elapsed });
        }
    }
}

impl<'a> Control<'a> {
    pub(in crate::process) const fn new(fd: fd::BorrowedFd<'a>) -> Self {
        Self {
            fd,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn stopped(&self) -> Result<bool, Errno> {
        match Backend::recv_now::<Errno>(self.fd, &mut [0]) {
            Ok(_) => Ok(true),
            Err(error) if error.is_block() => Ok(false),
            Err(error) if error.is_intr() => {
                let mut entries = [poll::Entry::readable(self.fd)];
                match Backend::wait::<_, Errno>(&mut entries, 0) {
                    Ok(events) => Ok(events.count() != 0),
                    Err(error) if error.is_intr() => Ok(false),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    pub fn wait_stop_until(&self, deadline: time::Instant) -> Result<(), WaitError> {
        let started = time::Instant::now();
        wait(
            self.fd,
            started,
            deadline.saturating_duration_since(started),
        )
    }
}

impl fd::AsFd for Control<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd
    }
}

fn shutdown(socket: fd::BorrowedFd<'_>) -> Option<dope_core::Errno> {
    loop {
        match Backend::shutdown::<Errno>(socket, How::WRITE) {
            Ok(()) => return None,
            Err(errno) if errno.is_intr() => {}
            Err(errno) => return Some(errno),
        }
    }
}

impl Worker {
    pub fn run<T, E>(
        self,
        run: impl for<'a> FnOnce(process::Shutdown<'a>) -> Result<T, E>,
    ) -> Outcome<T, E> {
        let output = run(process::Shutdown::new(self.tx()));
        let event = if output.is_ok() {
            bus::Event::Done
        } else {
            bus::Event::Failed
        };
        let report = match self.send(event) {
            Ok(()) => bus::Report::Clean,
            Err(fault) => bus::Report::Failed(fault),
        };
        match (output, report) {
            (Ok(value), bus::Report::Clean) if !matches!(self.phase.get(), Phase::Failed) => {
                Outcome::Done(value)
            }
            (output, report) => Outcome::Failed { output, report },
        }
    }

    pub(in crate::process) const fn tx(&self) -> Tx<'_> {
        Tx(self, marker::PhantomData)
    }

    fn send(&self, event: bus::Event) -> Result<(), bus::Fault> {
        if let Some(failure) = self.failure.get() {
            return Err(failure);
        }
        if matches!(self.phase.get(), Phase::Failed) {
            return Ok(());
        }
        if event == bus::Event::Failed {
            self.phase.set(Phase::Failed);
        }
        let result = loop {
            match Backend::send_quiet::<Errno>(self.socket.as_fd(), &[event.encode()]) {
                Ok([_]) => break Ok(()),
                Ok(bytes) => {
                    break Err(bus::Write::Record(bus::RecordSize {
                        expected: 1,
                        actual: bytes.len(),
                    }));
                }
                Err(error) if error.is_intr() => {}
                Err(error) => break Err(bus::Write::Syscall(error)),
            }
        };
        let shutdown = if result.is_err() || event == bus::Event::Failed {
            shutdown(self.socket.as_fd())
        } else {
            None
        };
        let (failure, shutdown) = match (result, shutdown) {
            (Ok(()), None) => return Ok(()),
            (Ok(()), Some(error)) => (bus::Write::Shutdown(error), None),
            (Err(failure), shutdown) => (failure, shutdown),
        };
        let fault = bus::Fault {
            event,
            failure,
            shutdown,
        };
        self.failure.set(Some(fault));
        Err(fault)
    }

    fn report(&self) -> bus::Report {
        match self.failure.get() {
            Some(failure) => bus::Report::Failed(failure),
            None => bus::Report::Clean,
        }
    }
}

impl<'a> Tx<'a> {
    /// Publishes worker installation once across every session of this exact peer.
    pub(in crate::process) fn installed(self) -> Result<(), bus::Fault> {
        let peer = self.0;
        if !matches!(peer.phase.get(), Phase::New) {
            return peer.failure.get().map_or(Ok(()), Err);
        }
        peer.send(bus::Event::Installed)?;
        peer.phase.set(Phase::Installed);
        Ok(())
    }

    pub(in crate::process) fn send(self, event: bus::Event) -> Result<(), bus::Fault> {
        self.0.send(event)
    }

    pub(in crate::process) fn report(self) -> bus::Report {
        self.0.report()
    }

    pub(in crate::process) fn terminal(
        self,
        _: shutdown::Requested<process::Shutdown<'a>>,
    ) -> Result<bus::Event, bus::Fault> {
        match self.report() {
            bus::Report::Failed(failure) => Err(failure),
            bus::Report::Clean => Ok(if matches!(self.0.phase.get(), Phase::Failed) {
                bus::Event::Failed
            } else {
                bus::Event::Done
            }),
        }
    }

    pub(in crate::process) fn as_fd(&self) -> fd::BorrowedFd<'a> {
        self.0.socket.as_fd()
    }
}

const _: () = {
    assert!(mem::size_of::<Tx<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Tx<'static>>() == mem::align_of::<usize>());
};
