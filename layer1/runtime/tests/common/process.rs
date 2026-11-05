use std::{cell, convert::Infallible, mem, ops};

use dope_core::{
    Backend, Errno,
    dope_sys::kernel,
    driver::life::{creation::Attempt, recovery},
    platform::{affinity, affinity::Cpu},
};
use dope_runtime::{
    executor::{Build as _, session, session::Completion},
    process::{
        self,
        failure::{bus, execute},
        watch,
    },
};
use dope_test::alloc::Tracker;
use o3::{alloc, base::time::Positive, depot::array::init, mem::slot};

use crate::common::{
    runtime::topology::PROFILE_CONFIG,
    scenario::{app::Empty, rt::Configured},
};

pub fn repeated_install() {
    let cpus = <[Cpu; 1]>::try_current().expect("one available worker CPU");
    for heartbeat in [false, true] {
        let runtime = process::Layout::pinned(cpus)
            .and_then(|layout| layout.try_assign([()]))
            .expect("one process worker");
        let mut storage = mem::MaybeUninit::uninit();
        let result = runtime.watch_gated(
            slot::Vacant::new(&mut storage),
            |(), context, gate| {
                Ok::<_, Infallible>(worker(context, |mut scope| {
                    let mut finish = Completion::new();
                    for _ in 0..3 {
                        scope
                            .with_app(&mut finish, Empty {}, |_| ())
                            .expect("repeated installation");
                    }
                    scope
                        .with_app(&mut finish, Empty {}, |app| {
                            let drain = gate.open().expect("worker gate");
                            let request = app.run().expect("worker shutdown");
                            drain.finish().expect("worker drain");
                            request
                        })
                        .unwrap_or_else(|error| panic!("worker session: {:?}", error.cause()))
                }))
            },
            Watch::new(heartbeat),
        );
        assert!(
            result.is_ok(),
            "process installation, heartbeat {heartbeat}: {result:?}"
        );
    }
}

pub fn workers_shutdown() {
    let cpus = <[Cpu; 2]>::try_current().expect("two available worker CPUs");
    let runtime = process::Layout::pinned(cpus)
        .and_then(|layout| layout.try_assign([(); 2]))
        .expect("two process workers");
    let mut storage = mem::MaybeUninit::uninit();
    let result = runtime.watch_gated(
        slot::Vacant::new(&mut storage),
        |(), context, gate| {
            Ok::<_, Infallible>(worker(context, |mut scope| {
                let mut finish = Completion::new();
                scope
                    .with_app(&mut finish, Empty {}, |app| {
                        let drain = gate.open().expect("worker gate");
                        let request = app.run().expect("worker shutdown");
                        drain.finish().expect("worker drain");
                        request
                    })
                    .unwrap_or_else(|error| panic!("worker session: {:?}", error.cause()))
            }))
        },
        Watch::new(false),
    );
    assert!(result.is_ok(), "process workers shutdown: {result:?}");
}

fn worker<'run, R>(
    context: process::Context<'run>,
    run: impl for<'scope, 'life, 'p, 'd> FnOnce(
        session::Scope<'scope, 'life, 'p, 'd, (), process::Shutdown<'run>>,
    ) -> R,
) -> R {
    let mut create = Attempt::new();
    let executor = Configured::quic::<2, 8>()
        .try_source(&mut create, context)
        .unwrap_or_else(|error| panic!("worker executor: {:?}", error.kind()));
    let mut reported = None;
    let request = executor.run(|executor| {
        executor
            .enter_with(
                run,
                recovery::Policy::new(
                    |event| {
                        reported.get_or_insert(event);
                    },
                    crate::common::recover,
                ),
            )
            .unwrap_or_else(|error| panic!("worker teardown: {:?}", error.cause()))
    });
    assert!(reported.is_none(), "native reclamation: {reported:?}");
    request
}

struct Watch {
    heartbeat: bool,
    pulsed: cell::Cell<bool>,
}

impl Watch {
    fn new(heartbeat: bool) -> Self {
        Self {
            heartbeat,
            pulsed: cell::Cell::new(false),
        }
    }
}

impl watch::Notify for Watch {
    type Error = Error;
    type Heartbeat = Self;

    fn ready(self) -> Result<Self, Error> {
        if !self.heartbeat {
            try_terminate().map_err(Error::Signal)?;
        }
        Ok(self)
    }
}

impl watch::Heartbeat<Error> for Watch {
    fn period(&self) -> Positive {
        if self.heartbeat {
            Positive::from_nanos::<1>()
        } else {
            Positive::from_secs::<1>()
        }
    }

    fn pulse(&self) -> Result<(), Error> {
        if self.heartbeat && !self.pulsed.replace(true) {
            try_terminate().map_err(Error::Signal)?;
        }
        Ok(())
    }

    fn draining(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub fn terminal_failure() {
    let cpus = <[Cpu; 2]>::try_current().expect("two available worker CPUs");
    let failed = cpus[0];
    for signal in [false, true] {
        let runtime = process::Layout::pinned(cpus)
            .and_then(|layout| layout.try_assign(cpus))
            .expect("two process workers");
        let mut storage = mem::MaybeUninit::uninit();
        let error = runtime
            .run(slot::Vacant::new(&mut storage), |cpu, context| {
                assert_eq!(context.cpu(), cpu);
                let terminal = context.terminal();
                if signal && cpu == failed {
                    try_terminate().expect("pre-install termination");
                }
                worker(context, |mut scope| {
                    let mut finish = Completion::new();
                    scope
                        .with_app(&mut finish, Empty {}, |app| {
                            if cpu == failed {
                                let error = terminal.fail();
                                let request = app.run().expect("worker shutdown");
                                if signal {
                                    assert!(matches!(error, Ok(bus::Event::Failed)));
                                    Ok(request)
                                } else {
                                    Err(error)
                                }
                            } else {
                                Ok(app.run().expect("companion shutdown"))
                            }
                        })
                        .unwrap_or_else(|error| panic!("worker session: {:?}", error.cause()))
                })
            })
            .expect_err("reported entry failure");
        let faults = error.fold(Vec::new(), |mut faults, fault| {
            faults.push(fault);
            faults
        });
        if signal {
            assert!(matches!(
                faults.as_slice(),
                [execute::Fault::Worker(execute::Worker::Failed { cpu, handoff: None })]
                    if *cpu == failed
            ));
        } else {
            assert_eq!(faults.len(), 2);
            assert!(faults.iter().any(|fault| matches!(
                fault,
                execute::Fault::Control(execute::Cause::WorkerFailed { cpu }) if *cpu == failed
            )));
            assert!(faults.iter().any(|fault| matches!(
                fault,
                execute::Fault::Worker(execute::Worker::Entry {
                    cpu,
                    source: Ok(bus::Event::Failed),
                    bus: bus::Report::Clean,
                    handoff: None,
                }) if *cpu == failed
            )));
        }
    }
}

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
                            recovery::Policy::new(drop, crate::common::recover),
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
    Signal(Errno),
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
