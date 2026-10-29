use std::{cell, mem, ops, thread, time};

use dope_core::{
    dope_sys::kernel,
    platform::{affinity, affinity::Cpu, channel::Channel, wake},
};
use dope_runtime::process::watch;
use o3::{base::time::Positive, depot::array::init};

pub trait Cpus: Sized {
    fn try_current() -> Result<Self, Error>;
}

impl<const N: usize> Cpus for [Cpu; N] {
    fn try_current() -> Result<Self, Error> {
        let mut cpus = Cpu::current().map_err(Error::Discovery)?;
        let actual = cpus.len();
        let mut output = mem::MaybeUninit::uninit();
        let mut guard = init::Guard::<_, N>::new(&mut output);
        loop {
            let vacant = match guard.next() {
                ops::ControlFlow::Break(full) => return Ok(full.into_array()),
                ops::ControlFlow::Continue(vacant) => vacant,
            };
            let cpu = cpus.next().ok_or(Error::Count {
                actual,
                required: N,
            })?;
            guard = vacant.insert(cpu);
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Discovery(affinity::Error),
    Count { actual: usize, required: usize },
    Wait(wake::WaitError),
    Signal(dope_core::Errno),
    Starved,
}

pub struct Resources {
    pub values: ops::Range<usize>,
    pub declared: usize,
}

impl Iterator for Resources {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        self.values.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.declared, Some(self.declared))
    }
}

impl ExactSizeIterator for Resources {}

pub struct Input<'a>(pub &'a cell::Cell<Option<usize>>);

impl Drop for Input<'_> {
    fn drop(&mut self) {
        self.0
            .set(self.0.get().and_then(|drops| drops.checked_add(1)));
    }
}

pub enum Ordered {
    First(Channel),
    Last(Channel),
}

thread_local! {
    static EXIT: cell::Cell<Option<Channel>> = const { cell::Cell::new(None) };
}

pub struct Stop;

pub struct Restart {
    pub ready: wake::Wait,
    pub heartbeat: bool,
}

impl watch::Notify for Restart {
    type Error = Error;
    type Heartbeat = Pulse;

    fn ready(self) -> Result<Pulse, Error> {
        self.ready.wait().map_err(Error::Wait)?;
        if !self.heartbeat {
            Stop.ready()?;
        }
        Ok(Pulse {
            enabled: self.heartbeat,
            pulsed: cell::Cell::new(false),
            draining: cell::Cell::new(false),
        })
    }
}

const PERIOD: Positive = Positive::from_secs::<1>();

impl watch::Notify for Stop {
    type Error = Error;
    type Heartbeat = Self;

    fn ready(self) -> Result<Self, Self::Error> {
        // SAFETY: target this manager thread after Runtime installed its signal receiver.
        let result = unsafe { libc::pthread_kill(libc::pthread_self(), libc::SIGTERM) };
        unsafe { kernel::raw::Status::new(result).try_pthread() }.map_err(Error::Signal)?;
        Ok(self)
    }
}

impl watch::Heartbeat<Error> for Stop {
    fn period(&self) -> Positive {
        PERIOD
    }

    fn pulse(&self) -> Result<(), Error> {
        Ok(())
    }

    fn draining(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl Ordered {
    pub fn install(self) -> Result<Option<Channel>, thread::AccessError> {
        match self {
            Ordered::First(sender) => EXIT.try_with(|exit| {
                exit.set(Some(sender));
                None
            }),
            Ordered::Last(receiver) => Ok(Some(receiver)),
        }
    }
}

pub struct Pulse {
    enabled: bool,
    pulsed: cell::Cell<bool>,
    draining: cell::Cell<bool>,
}

impl watch::Heartbeat<Error> for Pulse {
    fn period(&self) -> Positive {
        if !self.enabled {
            return PERIOD;
        }
        Positive::from_nanos::<1>()
    }

    fn pulse(&self) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }
        if !self.pulsed.replace(true) {
            watch::Notify::ready(Stop)?;
            thread::sleep(time::Duration::from_millis(1));
        } else if !self.draining.get() {
            return Err(Error::Starved);
        }
        Ok(())
    }

    fn draining(&self) -> Result<(), Error> {
        self.draining.set(true);
        Ok(())
    }
}
