use std::any::Any;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::io::ErrorKind;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::mpsc::channel;
use std::thread::{Builder, scope};

use dope_core::driver::ext::DriverExt;
use o3::marker::ThreadBound;

use super::trigger::ShutdownTrigger;
use crate::DriverContext;
use crate::driver::Driver;
use crate::hash::Seed;

#[derive(Clone, Copy)]
enum Placement {
    Pinned(u16),
    Unbound,
}

/// Runtime information owned by one launcher worker.
pub struct WorkerContext {
    worker: usize,
    placement: Placement,
    seed: Seed,
    shutdown: ShutdownTrigger,
    _thread: ThreadBound,
}

impl WorkerContext {
    pub const fn worker(&self) -> usize {
        self.worker
    }

    pub const fn cpu(&self) -> Option<u16> {
        match self.placement {
            Placement::Pinned(cpu) => Some(cpu),
            Placement::Unbound => None,
        }
    }

    pub const fn seed(&self) -> Seed {
        self.seed
    }

    /// Clones the shared launcher shutdown handle for APIs that register a
    /// trigger directly instead of accepting a `WorkerContext`.
    pub fn shutdown_trigger(&self) -> io::Result<ShutdownTrigger> {
        self.shutdown.try_clone()
    }

    /// Registers the launcher's shared shutdown source with this worker's driver.
    pub fn try_register_shutdown(&self, driver: &mut DriverContext<'_, '_>) -> io::Result<()> {
        self.shutdown.try_register(driver)
    }
}

pub trait WorkerEntry {
    type Input: Send;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()>;
}

/// Supervises a fixed set of runtime worker threads.
///
/// The first worker to finish causes the shared shutdown source to fire. Other
/// workers must register [`WorkerContext::try_register_shutdown`] to participate
/// in cooperative fail-fast shutdown.
pub struct Launcher {
    placements: Vec<Placement>,
    shutdown: ShutdownTrigger,
    worker_stack_size: usize,
}

const DEFAULT_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;

enum WorkerOutcome {
    Success,
    Failed(io::Error),
    Panicked(Box<dyn Any + Send>),
}

struct WorkerReport {
    worker: usize,
    outcome: WorkerOutcome,
}

enum LaunchOutcome {
    Success,
    Panicked(Box<dyn Any + Send>),
}

struct WorkerFailure {
    worker: usize,
    source: io::Error,
}

struct SpawnFailure {
    spawn: io::Error,
    shutdown: Option<io::Error>,
    worker: Option<WorkerFailure>,
}

impl Display for SpawnFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "launcher could not spawn every worker: {}", self.spawn)?;
        if let Some(shutdown) = &self.shutdown {
            write!(f, "; shutdown notification failed: {shutdown}")?;
        }
        if let Some(worker) = &self.worker {
            write!(f, "; a started {worker}")?;
        }
        Ok(())
    }
}

impl Debug for SpawnFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnFailure")
            .field("spawn", &self.spawn)
            .field("shutdown", &self.shutdown)
            .field("worker", &self.worker)
            .finish()
    }
}

impl Error for SpawnFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.spawn)
    }
}

impl Display for WorkerFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "launcher worker {}: {}", self.worker, self.source)
    }
}

impl Debug for WorkerFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerFailure")
            .field("worker", &self.worker)
            .field("source", &self.source)
            .finish()
    }
}

impl Error for WorkerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

struct WorkerLedger {
    seen: Box<[bool]>,
    reported: usize,
    failure: Option<WorkerFailure>,
    panic: Option<Box<dyn Any + Send>>,
    protocol: Option<io::Error>,
}

impl WorkerLedger {
    fn new(workers: usize) -> Self {
        Self {
            seen: vec![false; workers].into_boxed_slice(),
            reported: 0,
            failure: None,
            panic: None,
            protocol: None,
        }
    }

    fn record(&mut self, report: WorkerReport) {
        let Some(seen) = self.seen.get_mut(report.worker) else {
            self.protocol.get_or_insert_with(|| {
                io::Error::other(format!(
                    "launcher worker report index {} is out of range",
                    report.worker
                ))
            });
            return;
        };
        if *seen {
            self.protocol.get_or_insert_with(|| {
                io::Error::other(format!(
                    "launcher worker {} reported more than once",
                    report.worker
                ))
            });
            return;
        }
        *seen = true;
        self.reported += 1;
        match report.outcome {
            WorkerOutcome::Success => {}
            WorkerOutcome::Failed(source) if self.failure.is_none() => {
                self.failure = Some(WorkerFailure {
                    worker: report.worker,
                    source,
                });
            }
            WorkerOutcome::Failed(_) => {}
            WorkerOutcome::Panicked(payload) if self.panic.is_none() => {
                self.panic = Some(payload);
            }
            WorkerOutcome::Panicked(_) => {}
        }
    }

    fn finish(self) -> io::Result<LaunchOutcome> {
        if let Some(error) = self.protocol {
            return Err(error);
        }
        if self.reported != self.seen.len() {
            return Err(io::Error::other(format!(
                "launcher received {} of {} worker reports",
                self.reported,
                self.seen.len()
            )));
        }
        if let Some(payload) = self.panic {
            return Ok(LaunchOutcome::Panicked(payload));
        }
        match self.failure {
            None => Ok(LaunchOutcome::Success),
            Some(failure) => {
                let kind = failure.source.kind();
                Err(io::Error::new(kind, failure))
            }
        }
    }
}

impl Launcher {
    /// Returns the number of workers supervised by this launcher.
    pub fn worker_count(&self) -> usize {
        self.placements.len()
    }

    /// Returns the CPUs currently available to this process.
    pub fn allowed_cpus() -> io::Result<Vec<u16>> {
        Driver::allowed_cpus()
    }

    /// Pins the current thread to one CPU.
    pub fn pin_to_cpu(cpu: u16) -> io::Result<()> {
        Driver::init_thread(cpu)
    }

    /// Creates workers that are each pinned to one distinct CPU.
    pub fn pinned(cpus: Vec<u16>) -> io::Result<Self> {
        if cpus.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Launcher::pinned requires at least one CPU",
            ));
        }
        let mut unique = HashSet::with_capacity(cpus.len());
        if cpus.iter().any(|cpu| !unique.insert(*cpu)) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Launcher::pinned requires distinct CPUs",
            ));
        }
        Self::with_placements(cpus.into_iter().map(Placement::Pinned).collect())
    }

    /// Creates workers without hard CPU affinity.
    pub fn unbound(workers: usize) -> io::Result<Self> {
        if workers == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Launcher::unbound requires at least one worker",
            ));
        }
        Self::with_placements(vec![Placement::Unbound; workers])
    }

    fn with_placements(placements: Vec<Placement>) -> io::Result<Self> {
        Ok(Self {
            placements,
            shutdown: ShutdownTrigger::new()?,
            worker_stack_size: DEFAULT_WORKER_STACK_SIZE,
        })
    }

    /// Sets the reserved stack size of each worker thread.
    ///
    /// Runtime applications commonly contain fixed-capacity driver and protocol state. Keeping
    /// this setting on the launcher makes their stack requirement explicit and avoids depending
    /// on the platform's comparatively small thread default.
    pub fn worker_stack_size(mut self, bytes: usize) -> io::Result<Self> {
        if bytes == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Launcher::worker_stack_size requires a non-zero size",
            ));
        }
        self.worker_stack_size = bytes;
        Ok(self)
    }

    /// Returns a handle that can stop all cooperative workers from another thread.
    pub fn shutdown_trigger(&self) -> io::Result<ShutdownTrigger> {
        self.shutdown.try_clone()
    }

    pub fn run<E>(self, inputs: Vec<E::Input>) -> io::Result<()>
    where
        E: WorkerEntry,
    {
        let Self {
            placements,
            shutdown,
            worker_stack_size,
        } = self;
        if placements.len() != inputs.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Launcher::run requires one input per worker",
            ));
        }

        Driver::init_process()?;
        let seed = Seed::random()?;
        let worker_count = placements.len();
        let mut workers = Vec::with_capacity(worker_count);
        for (worker, (placement, input)) in placements.into_iter().zip(inputs).enumerate() {
            workers.push((
                worker,
                placement,
                input,
                seed.derive(worker as u64),
                shutdown.try_clone()?,
            ));
        }

        let outcome = scope(|scope| -> io::Result<LaunchOutcome> {
            let (completed, outcomes) = channel();
            let mut handles = Vec::with_capacity(worker_count);
            for (worker, placement, input, seed, worker_shutdown) in workers {
                let completed = completed.clone();
                let handle = Builder::new()
                    .name(format!("dope-worker-{worker}"))
                    .stack_size(worker_stack_size)
                    .spawn_scoped(scope, move || {
                        let outcome = match catch_unwind(AssertUnwindSafe(|| {
                            enter::<E>(worker, placement, seed, worker_shutdown, input)
                        })) {
                            Ok(Ok(())) => WorkerOutcome::Success,
                            Ok(Err(error)) => WorkerOutcome::Failed(error),
                            Err(payload) => WorkerOutcome::Panicked(payload),
                        };
                        let report = WorkerReport { worker, outcome };
                        completed.send(report).map_err(|error| error.0)
                    });
                match handle {
                    Ok(handle) => handles.push((worker, handle)),
                    Err(spawn) => {
                        let shutdown_error = shutdown.fire().err();
                        let mut worker_error = None;
                        let mut worker_panic = None;
                        for (worker, handle) in handles {
                            match handle.join() {
                                Ok(Ok(())) => {}
                                Ok(Err(report)) => match report.outcome {
                                    WorkerOutcome::Success => {}
                                    WorkerOutcome::Failed(source) if worker_error.is_none() => {
                                        worker_error = Some(WorkerFailure { worker, source });
                                    }
                                    WorkerOutcome::Failed(_) => {}
                                    WorkerOutcome::Panicked(payload) if worker_panic.is_none() => {
                                        worker_panic = Some(payload);
                                    }
                                    WorkerOutcome::Panicked(_) => {}
                                },
                                Err(payload) if worker_panic.is_none() => {
                                    worker_panic = Some(payload);
                                }
                                Err(_) => {}
                            }
                        }
                        if let Some(payload) = worker_panic {
                            resume_unwind(payload);
                        }
                        let kind = spawn.kind();
                        return Err(io::Error::new(
                            kind,
                            SpawnFailure {
                                spawn,
                                shutdown: shutdown_error,
                                worker: worker_error,
                            },
                        ));
                    }
                }
            }
            drop(completed);

            let mut ledger = WorkerLedger::new(worker_count);
            let first = outcomes.recv().map_err(|_| {
                io::Error::other("launcher workers exited without reporting an outcome")
            })?;
            ledger.record(first);
            let fire_result = shutdown.fire();

            for _ in 1..worker_count {
                let report = outcomes.recv().map_err(|_| {
                    io::Error::other("launcher worker outcome channel closed early")
                })?;
                ledger.record(report);
            }
            for (worker, handle) in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(report)) => ledger.record(report),
                    Err(payload) => ledger.record(WorkerReport {
                        worker,
                        outcome: WorkerOutcome::Panicked(payload),
                    }),
                }
            }

            let outcome = ledger.finish()?;
            if matches!(&outcome, LaunchOutcome::Success) {
                fire_result?;
            }
            Ok(outcome)
        })?;
        match outcome {
            LaunchOutcome::Success => Ok(()),
            LaunchOutcome::Panicked(payload) => resume_unwind(payload),
        }
    }
}

fn enter<E>(
    worker: usize,
    placement: Placement,
    seed: Seed,
    shutdown: ShutdownTrigger,
    input: E::Input,
) -> io::Result<()>
where
    E: WorkerEntry,
{
    if let Placement::Pinned(cpu) = placement {
        Launcher::pin_to_cpu(cpu)?;
    }
    E::run(
        input,
        WorkerContext {
            worker,
            placement,
            seed,
            shutdown,
            _thread: ThreadBound::NEW,
        },
    )
}
