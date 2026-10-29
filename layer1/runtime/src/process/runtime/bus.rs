use std::{
    cell, marker, mem,
    os::fd::{self, AsFd as _},
};

use dope_core::{
    dope_sys::io::socket::record::{Fault, Record},
    platform::channel::{self, Channel},
};

use crate::process::failure::bus;

pub(in crate::process) struct Main(Channel);

pub(in crate::process) struct Bus {
    socket: Channel,
    failure: cell::Cell<Option<bus::Fault>>,
    installed: cell::Cell<bool>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::process) struct Tx<'a>(&'a Bus, marker::PhantomData<fn(&'a ()) -> &'a ()>);

/// Selects the worker event sequence from its `Plain` or `Gated` entry type.
pub(in crate::process::runtime) trait Mode {
    /// Whether the worker publishes application ready and drained events.
    const GATED: bool;
}

impl Main {
    pub(in crate::process) fn try_pair() -> Result<(Self, Bus), channel::Create> {
        let (left, right) = Channel::pair()?;
        Ok((
            Self(left),
            Bus {
                socket: right,
                failure: cell::Cell::new(None),
                installed: cell::Cell::new(false),
            },
        ))
    }

    pub(in crate::process::runtime) fn recv(&self) -> Result<bus::Event, bus::Read> {
        match self.0.recv() {
            Record::Value(value) => bus::Event::decode(value).map_err(bus::Read::Event),
            Record::Closed => Err(bus::Read::Closed),
            Record::Failed(Fault::Syscall(source)) => Err(bus::Read::Syscall(source)),
            Record::Failed(Fault::Record(size)) => Err(bus::Read::Record(bus::RecordSize {
                expected: size.expected,
                actual: size.actual,
            })),
        }
    }

    pub(in crate::process::runtime) fn handoff<M: Mode>(&self) -> Option<bus::Read> {
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
                            (Some(Event::Installed), Event::Done) => !M::GATED,
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

    pub(in crate::process::runtime) fn stop(&self) -> Option<dope_core::Errno> {
        shutdown(&self.0)
    }

    pub(in crate::process) fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

fn shutdown(socket: &Channel) -> Option<dope_core::Errno> {
    loop {
        match socket.shutdown() {
            Ok(()) => return None,
            Err(errno) if errno.is_intr() => {}
            Err(errno) => return Some(errno),
        }
    }
}

impl Bus {
    pub(in crate::process) const fn tx(&self) -> Tx<'_> {
        Tx(self, marker::PhantomData)
    }

    fn send(&self, event: bus::Event) -> Result<(), bus::Fault> {
        if let Some(failure) = self.failure.get() {
            return Err(failure);
        }
        let failure = match self.socket.send(event.encode()) {
            Ok(()) => return Ok(()),
            Err(Fault::Syscall(source)) => bus::Write::Syscall(source),
            Err(Fault::Record(size)) => bus::Write::Record(bus::RecordSize {
                expected: size.expected,
                actual: size.actual,
            }),
        };
        let fault = bus::Fault {
            event,
            failure,
            shutdown: shutdown(&self.socket),
        };
        self.failure.set(Some(fault));
        Err(fault)
    }

    fn report(&self) -> bus::Report {
        match self.failure.take() {
            Some(failure) => bus::Report::Failed(failure),
            None => bus::Report::Clean,
        }
    }
}

impl<'a> Tx<'a> {
    /// Publishes worker installation once across every session of this exact peer.
    pub(in crate::process) fn installed(self) -> Result<(), bus::Fault> {
        let peer = self.0;
        if peer.installed.get() {
            return peer.failure.get().map_or(Ok(()), Err);
        }
        peer.send(bus::Event::Installed)?;
        peer.installed.set(true);
        Ok(())
    }

    pub(in crate::process) fn send(self, event: bus::Event) -> Result<(), bus::Fault> {
        self.0.send(event)
    }

    pub(in crate::process) fn report(self) -> bus::Report {
        self.0.report()
    }

    pub(in crate::process) fn as_fd(&self) -> fd::BorrowedFd<'a> {
        self.0.socket.as_fd()
    }
}

const _: () = {
    assert!(mem::size_of::<Tx<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Tx<'static>>() == mem::align_of::<usize>());
};
