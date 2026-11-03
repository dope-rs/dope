use std::{cell, convert::Infallible, mem, ops};

use dope_core::{
    Backend, Errno,
    dope_sys::kernel,
    driver::life::{creation::Attempt, recovery},
    platform::{affinity, affinity::Cpu},
};
use dope_runtime::{
    executor::{Build as _, session::Completion},
    process::{self, failure::execute, watch},
};
use dope_test::alloc::Tracker;
use o3::{alloc, base::time::Positive, depot::array::init, mem::slot};

use crate::common::{runtime::topology::PROFILE_CONFIG, scenario::app::Empty};

pub fn cancel_gated_startup() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPU");
    for cancellation in [
        Cancellation::BeforeInstall,
        Cancellation::AfterInstall,
        Cancellation::ObserverFailure,
    ] {
        let mut storage = mem::MaybeUninit::uninit();
        let runtime = process::Layout::pinned([cpu])
            .and_then(|layout| layout.try_assign([cancellation]))
            .expect("one process worker");
        let run = || {
            runtime.watch_gated(
                slot::Vacant::new(&mut storage),
                |cancellation, context, gate| {
                    if matches!(cancellation, Cancellation::BeforeInstall) {
                        // Installed cannot be published before this signal. The control
                        // set registers the signal first and selects the first ready
                        // descriptor, leaving Installed unread for the handoff.
                        try_terminate().expect("pre-install termination");
                    }
                    let mut create = Attempt::new();
                    let executor = context.try_build(&mut create, PROFILE_CONFIG);
                    executor
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("worker executor");
                    let request = executor.ok().expect("validated executor").run(|executor| {
                        let result = executor.enter_with(
                            |mut scope| {
                                let mut finish = Completion::new();
                                let result = scope.with_app(&mut finish, Empty {}, |app| {
                                    if matches!(cancellation, Cancellation::AfterInstall) {
                                        try_terminate().expect("installed termination");
                                    }
                                    drop(gate);
                                    app.run().expect("worker shutdown")
                                });
                                result
                                    .as_ref()
                                    .map_err(|error| error.cause())
                                    .expect("worker application");
                                result.ok().expect("validated application")
                            },
                            recovery::Policy::new(
                                drop,
                                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                                    panic!("native recovery: {:?}", failure.cause());
                                },
                            ),
                        );
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("worker teardown");
                        result.ok().expect("validated teardown")
                    });
                    Ok::<_, Infallible>(request)
                },
                ClosedWatch,
            )
        };
        match cancellation {
            Cancellation::ObserverFailure => {
                // Admit worker slots and failure storage, then reject the observer's
                // poll set before it can consume the worker's Installed event.
                let error = Tracker::fail_after(2, run).expect_err("observer allocation");
                let failures = error.fold(0, |count, failure| {
                    assert!(matches!(
                        failure,
                        execute::Fault::Control(execute::Cause::Allocation(
                            alloc::Error::Exhausted
                        ))
                    ));
                    count + 1
                });
                assert_eq!(failures, 1);
            }
            _ => {
                let result = run();
                assert!(
                    result.is_ok(),
                    "startup cancellation {cancellation:?}: {result:?}"
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Cancellation {
    BeforeInstall,
    AfterInstall,
    ObserverFailure,
}

pub struct ClosedWatch;

impl watch::Notify for ClosedWatch {
    type Error = Error;
    type Heartbeat = Self;

    fn ready(self) -> Result<Self::Heartbeat, Self::Error> {
        Err(Error::Ready)
    }
}

impl watch::Heartbeat<Error> for ClosedWatch {
    fn period(&self) -> Positive {
        Positive::from_secs::<1>()
    }

    fn pulse(&self) -> Result<(), Error> {
        Err(Error::Pulse)
    }

    fn draining(&self) -> Result<(), Error> {
        Err(Error::Drain)
    }
}

pub fn try_terminate() -> Result<(), Errno> {
    // SAFETY: the isolated test process owns its installed SIGTERM receiver.
    let result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    // SAFETY: kill returns zero or -1 with errno on this thread.
    unsafe {
        kernel::raw::Status::new(result)
            .try_zero::<Errno>(|| kernel::raw::Code::last::<Backend>().into())
    }
}

pub fn try_limit() -> Result<(libc::rlim_t, libc::rlim_t), Errno> {
    let mut limit = mem::MaybeUninit::uninit();
    // SAFETY: the output points to storage for one complete resource limit.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    // SAFETY: getrlimit returns zero or -1 with this thread's errno.
    unsafe {
        kernel::raw::Status::new(result)
            .try_zero::<Errno>(|| kernel::raw::Code::last::<Backend>().into())
    }?;
    // SAFETY: the successful query initialized both limit fields.
    let limit = unsafe { limit.assume_init() };
    Ok((limit.rlim_cur, limit.rlim_max))
}

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
    Ready,
    Pulse,
    Drain,
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
