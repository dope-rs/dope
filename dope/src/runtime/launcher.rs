use std::collections::HashSet;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;

use o3::marker::ThreadBound;

use super::ShutdownTrigger;
use crate::hash::Seed;
use crate::{Driver, DriverContext};
use dope_core::driver::ext::DriverExt;

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
                io::ErrorKind::InvalidInput,
                "Launcher::pinned requires at least one CPU",
            ));
        }
        let mut unique = HashSet::with_capacity(cpus.len());
        if cpus.iter().any(|cpu| !unique.insert(*cpu)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Launcher::pinned requires distinct CPUs",
            ));
        }
        Self::with_placements(cpus.into_iter().map(Placement::Pinned).collect())
    }

    /// Creates workers without hard CPU affinity.
    pub fn unbound(workers: usize) -> io::Result<Self> {
        if workers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Launcher::unbound requires at least one worker",
            ));
        }
        Self::with_placements(vec![Placement::Unbound; workers])
    }

    pub fn from_affinity() -> io::Result<Self> {
        Self::pinned(Self::allowed_cpus()?)
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
                io::ErrorKind::InvalidInput,
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
                io::ErrorKind::InvalidInput,
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

        thread::scope(|scope| -> io::Result<()> {
            let (completed, outcomes) = mpsc::channel();
            let mut handles = Vec::with_capacity(worker_count);
            for (worker, placement, input, seed, worker_shutdown) in workers {
                let completed = completed.clone();
                handles.push(
                    thread::Builder::new()
                        .name(format!("dope-worker-{worker}"))
                        .stack_size(worker_stack_size)
                        .spawn_scoped(scope, move || {
                            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                enter::<E>(worker, placement, seed, worker_shutdown, input)
                            }))
                            .unwrap_or_else(|_| {
                                Err(io::Error::other(format!(
                                    "launcher worker {worker} panicked"
                                )))
                            });
                            let _ = completed.send((worker, result));
                        })?,
                );
            }
            drop(completed);

            let mut results = (0..worker_count)
                .map(|_| None)
                .collect::<Vec<Option<io::Result<()>>>>();
            let (first_worker, first_result) = outcomes.recv().map_err(|_| {
                io::Error::other("launcher workers exited without reporting an outcome")
            })?;
            results[first_worker] = Some(first_result);
            let fire_result = shutdown.fire();

            for _ in 1..worker_count {
                let (worker, result) = outcomes.recv().map_err(|_| {
                    io::Error::other("launcher worker outcome channel closed early")
                })?;
                results[worker] = Some(result);
            }
            for handle in handles {
                handle
                    .join()
                    .map_err(|_| io::Error::other("launcher supervisor invariant violated"))?;
            }

            for result in results {
                result.expect("every worker reported its outcome")?;
            }
            fire_result
        })
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
