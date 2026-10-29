//! Worker execution and process-watch failures.
use dope_core::{
    dope_sys::thread::signal::Signal,
    platform::{affinity, channel, signal, thread},
};
use o3::alloc;

use crate::process::failure::{bus, config};

/// One process-control failure observed before worker return completes.
#[derive(Debug)]
pub enum Cause<S> {
    Config(config::Error),
    Allocation(alloc::Error),
    Signal(signal::Acquire),
    Entropy(dope_core::Errno),
    WorkerBus {
        cpu: affinity::Cpu,
        source: channel::Create,
    },
    BusRead {
        cpu: affinity::Cpu,
        source: bus::Read,
    },
    BusState(bus::State),
    WorkerFailed {
        cpu: affinity::Cpu,
    },
    WorkerMissing {
        cpu: affinity::Cpu,
    },
    WorkerSpawn {
        cpu: affinity::Cpu,
        source: thread::Error,
    },
    Poll(dope_core::Errno),
    TerminationWait(<signal::Source<'static> as Signal>::Error),
    WatchReady(S),
    WatchPulse(S),
    WatchDrain(S),
}

impl<S> Cause<S> {
    pub(in crate::process) fn received(
        result: Result<bus::Event, bus::Read>,
        cpu: affinity::Cpu,
    ) -> Result<bus::Event, Self> {
        match result {
            Ok(bus::Event::Failed) => Err(Self::WorkerFailed { cpu }),
            Ok(event) => Ok(event),
            Err(source) => Err(Self::BusRead { cpu, source }),
        }
    }
}

/// Exact bus failure observed while terminating one worker.
#[derive(Debug)]
pub enum Bus {
    Write {
        source: bus::Fault,
        terminal: Option<bus::Fault>,
        handoff: Option<bus::Read>,
    },
    Read(bus::Read),
}

/// Exact terminal failure produced by one returned worker.
#[derive(Debug)]
pub enum Worker<W> {
    Affinity {
        cpu: affinity::Cpu,
        source: affinity::Error,
        bus: bus::Report,
        terminal: Option<bus::Fault>,
        handoff: Option<bus::Read>,
    },
    Entry {
        cpu: affinity::Cpu,
        source: W,
        bus: bus::Report,
        terminal: Option<bus::Fault>,
        handoff: Option<bus::Read>,
    },
    Bus {
        cpu: affinity::Cpu,
        source: Bus,
    },
}

/// Every exact failure observed during one process run.
#[derive(Debug)]
pub enum Fault<'r, S, W> {
    Control(Cause<S>),
    Stop(dope_core::Errno),
    Release(signal::Release<'r>),
    Worker(Worker<W>),
}

/// Affine owner of all failures from one completed process run.
#[must_use = "every process failure must be consumed"]
#[derive(Debug)]
pub struct Error<'r, S, W> {
    control: Option<Cause<S>>,
    missing: Option<affinity::Cpu>,
    stop: Option<dope_core::Errno>,
    release: Option<signal::Release<'r>>,
    workers: Vec<Worker<W>>,
}

impl<'r, S, W> Error<'r, S, W> {
    pub(in crate::process) fn failed(
        control: Cause<S>,
        missing: Option<affinity::Cpu>,
        stop: Option<dope_core::Errno>,
        workers: Vec<Worker<W>>,
    ) -> Self {
        Self {
            control: Some(control),
            missing,
            stop,
            release: None,
            workers,
        }
    }

    pub(in crate::process) fn finish(
        missing: Option<affinity::Cpu>,
        stop: Option<dope_core::Errno>,
        workers: Vec<Worker<W>>,
    ) -> Result<(), Self> {
        if missing.is_none() && stop.is_none() && workers.is_empty() {
            return Ok(());
        }
        Err(Self {
            control: None,
            missing,
            stop,
            release: None,
            workers,
        })
    }

    pub fn fold<R>(self, mut state: R, mut apply: impl FnMut(R, Fault<'r, S, W>) -> R) -> R {
        if let Some(cause) = self.control {
            state = apply(state, Fault::Control(cause));
        }
        if let Some(cpu) = self.missing {
            state = apply(state, Fault::Control(Cause::WorkerMissing { cpu }));
        }
        if let Some(source) = self.stop {
            state = apply(state, Fault::Stop(source));
        }
        let state = self
            .workers
            .into_iter()
            .fold(state, |state, failure| apply(state, Fault::Worker(failure)));
        match self.release {
            Some(cause) => apply(state, Fault::Release(cause)),
            None => state,
        }
    }
}

impl<'r, S, W> From<Cause<S>> for Error<'r, S, W> {
    fn from(cause: Cause<S>) -> Self {
        Self::failed(cause, None, None, Vec::new())
    }
}

impl<S> From<alloc::Error> for Cause<S> {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl<'r, S, W> From<signal::Error<'r, Error<'r, S, W>>> for Error<'r, S, W> {
    fn from(error: signal::Error<'r, Error<'r, S, W>>) -> Self {
        match error {
            signal::Error::Acquire(error) => {
                Self::failed(Cause::Signal(error), None, None, Vec::new())
            }
            signal::Error::Run(error) => error,
            signal::Error::Release { run, cause } => {
                let mut error = run.unwrap_or(Self {
                    control: None,
                    missing: None,
                    stop: None,
                    release: None,
                    workers: Vec::new(),
                });
                error.release = Some(cause);
                error
            }
        }
    }
}
