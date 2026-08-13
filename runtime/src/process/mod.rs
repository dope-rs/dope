use std::{cell, io, iter, mem, os::fd, process, thread};

use dope_core::{driver::settings, platform::affinity};

use crate::{executor, random, shutdown};

mod binding;
mod sealed;

pub(crate) use sealed::{Blocked, Limit, Set};

/// Allocation-free snapshot of the logical CPUs available to this process.
#[repr(transparent)]
pub struct Cpus(affinity::Cpus);

pub struct Context {
    affinity: binding::Binding,
    seed: random::Seed,
    shutdown: Shutdown,
}

/// Shutdown source owned by exactly one production worker runtime.
///
/// A proof issued by the explicit single-runtime shutdown API cannot be
/// retagged as a production process proof:
///
/// ```compile_fail,E0308
/// use dope_runtime::{process, shutdown};
///
/// fn retag(
///     requested: shutdown::Requested,
/// ) -> shutdown::Requested<process::Shutdown> {
///     requested
/// }
/// ```
pub struct Shutdown {
    source: fd::OwnedFd,
    ready: cell::Cell<Option<shutdown::Notify>>,
}

struct Trigger(shutdown::Notify);

/// Explicit stop authority for a process runtime started with [`Runtime::controlled`].
pub struct Control(shutdown::Notify);

enum Wait {
    Signal,
    Control(shutdown::Wait),
}

pub struct Runtime<I> {
    workers: Vec<Worker<I>>,
    triggers: Vec<Trigger>,
    ready: Vec<shutdown::Wait>,
    wait: Wait,
}

struct Worker<I> {
    cpu: u16,
    input: I,
    shutdown: Shutdown,
}

/// Armed proof that a worker may only return through its shutdown request.
struct Completion;

const STACK_SIZE: usize = 8 * 1024 * 1024;
const _: () = assert!(mem::size_of::<Completion>() == 0);

impl Cpus {
    pub fn current() -> io::Result<Self> {
        affinity::Cpus::current().map(Self)
    }
}

impl Iterator for Cpus {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for Cpus {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl ExactSizeIterator for Cpus {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl iter::FusedIterator for Cpus {}

const _: () = {
    assert!(std::mem::size_of::<Cpus>() == std::mem::size_of::<affinity::Cpus>());
    assert!(std::mem::align_of::<Cpus>() == std::mem::align_of::<affinity::Cpus>());
};

impl Context {
    pub fn cpu(&self) -> u16 {
        self.affinity.cpu()
    }
}

impl executor::Factory for Context {
    type Shutdown = Shutdown;

    fn executor(
        self,
        config: settings::Config,
    ) -> io::Result<executor::Executor<(), Self::Shutdown>> {
        executor::Executor::with_seed(config, self.seed)?
            .with_source(self.shutdown, |shutdown| fd::AsFd::as_fd(&shutdown.source))
    }
}

impl<I> Runtime<I> {
    pub fn pinned(workers: impl IntoIterator<Item = (u16, I)>) -> io::Result<Self> {
        Self::build(workers, Wait::Signal)
    }

    /// Builds a runtime whose process wait can be completed by the returned
    /// single-use control authority instead of a process signal.
    pub fn controlled(workers: impl IntoIterator<Item = (u16, I)>) -> io::Result<(Self, Control)> {
        let runtime = Self::build(workers, Wait::Signal)?;
        let (wait, notify) = shutdown::Ends::blocking()?.split();
        Ok((
            Self {
                wait: Wait::Control(wait),
                ..runtime
            },
            Control(notify),
        ))
    }

    fn build(workers: impl IntoIterator<Item = (u16, I)>, wait: Wait) -> io::Result<Self> {
        use std::collections::HashSet;

        let mut source = workers.into_iter();
        let Some(first) = source.next() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime requires at least one CPU",
            ));
        };
        Limit::current()?.raise()?;
        let (lower, _) = source.size_hint();
        let capacity = lower.saturating_add(1);
        let mut unique = HashSet::with_capacity(capacity);
        let mut values = Vec::with_capacity(capacity);
        let mut triggers = Vec::with_capacity(capacity);
        let mut ready = Vec::with_capacity(capacity);
        for (cpu, input) in iter::once(first).chain(source) {
            if !unique.insert(cpu) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "runtime requires distinct CPUs",
                ));
            }
            let (source, notify) = shutdown::Ends::event()?.split();
            let (ready_wait, ready_notify) = shutdown::Ends::blocking()?.split();
            values.push(Worker {
                cpu,
                input,
                shutdown: Shutdown {
                    source,
                    ready: cell::Cell::new(Some(ready_notify)),
                },
            });
            triggers.push(Trigger(notify));
            ready.push(ready_wait);
        }
        Ok(Self {
            workers: values,
            triggers,
            ready,
            wait,
        })
    }

    pub fn run<N>(
        self,
        entry: fn(I, Context) -> io::Result<shutdown::Requested<Shutdown>>,
        notify: N,
    ) -> io::Result<()>
    where
        I: Send,
        N: FnOnce() -> io::Result<()>,
    {
        let Self {
            workers,
            triggers,
            ready,
            wait,
        } = self;
        match wait {
            Wait::Signal => {
                let blocked = Set::termination()?.block()?;
                run_waiting(workers, triggers, ready, entry, notify, || blocked.wait())
            }
            Wait::Control(reader) => {
                run_waiting(workers, triggers, ready, entry, notify, || reader.wait())
            }
        }
    }
}

fn run_waiting<I>(
    workers: Vec<Worker<I>>,
    triggers: Vec<Trigger>,
    ready: Vec<shutdown::Wait>,
    entry: fn(I, Context) -> io::Result<shutdown::Requested<Shutdown>>,
    notify: impl FnOnce() -> io::Result<()>,
    wait: impl FnOnce() -> io::Result<()>,
) -> io::Result<()>
where
    I: Send,
{
    let seed = random::Seed::random()?;
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers.len());
        for (index, worker) in workers.into_iter().enumerate() {
            let seed_index = u64::try_from(index).map_err(io::Error::other)?;
            let worker_seed = seed.derive(seed_index);
            let spawn = thread::Builder::new()
                .name(format!("dope-core-{index}"))
                .stack_size(STACK_SIZE)
                .spawn_scoped(scope, move || execute(worker, worker_seed, entry));
            match spawn {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    stop(triggers);
                    join(handles);
                    return Err(error);
                }
            }
        }

        for worker in ready {
            if let Err(error) = worker.wait() {
                stop(triggers);
                join(handles);
                return Err(error);
            }
        }
        if let Err(error) = notify() {
            stop(triggers);
            join(handles);
            return Err(error);
        }
        let waited = wait();
        stop(triggers);
        join(handles);
        waited
    })
}

impl Shutdown {
    pub(crate) fn installed(&self) {
        let Some(ready) = self.ready.take() else {
            return;
        };
        if ready.notify().is_err() {
            process::abort();
        }
    }
}

impl Control {
    pub fn fire(self) -> io::Result<()> {
        self.0.notify()
    }
}

impl Completion {
    fn accept(self, _requested: shutdown::Requested<Shutdown>) {
        mem::forget(self);
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        eprintln!("dope-core: worker exited without shutdown completion");
        process::abort();
    }
}

fn execute<I>(
    worker: Worker<I>,
    seed: random::Seed,
    entry: fn(I, Context) -> io::Result<shutdown::Requested<Shutdown>>,
) where
    I: Send,
{
    let completion = Completion;
    let Worker {
        cpu,
        input,
        shutdown,
    } = worker;
    let result = binding::Binding::bind(cpu).and_then(|affinity| {
        entry(
            input,
            Context {
                affinity,
                seed,
                shutdown,
            },
        )
    });
    match result {
        Ok(requested) => completion.accept(requested),
        Err(error) => {
            eprintln!("dope-core cpu={cpu}: {error}");
            process::exit(1);
        }
    }
}

fn stop(triggers: Vec<Trigger>) {
    let mut failed = false;
    for trigger in triggers {
        if trigger.0.notify().is_err() {
            failed = true;
        }
    }
    if failed {
        process::exit(1);
    }
}

fn join(handles: Vec<thread::ScopedJoinHandle<'_, ()>>) {
    for handle in handles {
        if handle.join().is_err() {
            process::abort();
        }
    }
}
