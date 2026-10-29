//! Worker assignment, execution, and return state.
use std::iter;

use dope_core::{
    platform::{affinity, thread},
    random,
};
use o3::alloc::VecExt as _;

use crate::process::{
    STACK,
    failure::{bus::Read, execute},
    layout,
    runtime::worker,
};

pub(in crate::process::runtime) mod bound;
mod entries;
pub(in crate::process::runtime) mod task;
pub(super) use entries::{Entry, Gated, Plain};
use task::{Outcome, Output, Parent, Task};

const RANDOM_NAMESPACE: crate::random::Namespace = crate::random::Namespace::new(*b"\0worker\0");

struct Slots<'run, I, E, X> {
    values: thread::Workers<'run, Task<I, E, X>, Output<X>, Parent>,
    failures: Vec<execute::Worker<X>>,
}
#[must_use = "running workers must be stopped"]
pub(super) struct Running<'run, I, E, X> {
    slots: Slots<'run, I, E, X>,
}
#[must_use = "stopping workers must be returned"]
pub(super) struct Stopping<'run, I, E, X> {
    slots: Slots<'run, I, E, X>,
    stop: Option<dope_core::Errno>,
}

pub(in crate::process::runtime) fn scope<'r, I, E, X, S>(
    jobs: layout::Set<I>,
    seed: random::Seed,
    entry: E,
    run: impl for<'workers> FnOnce(Running<'workers, I, E, X>) -> Result<(), execute::Error<'r, S, X>>,
) -> Result<(), execute::Error<'r, S, X>>
where
    I: Send,
    E: worker::Entry<I, X>,
    X: Send,
{
    let (jobs, last) = jobs.into_parts();
    let len = jobs.len() + 1;
    let mut values = thread::Builder::try_new(len).map_err(execute::Cause::Allocation)?;
    let failures = Vec::try_new(len).map_err(execute::Cause::Allocation)?;
    let entries = iter::repeat_n(entry, len);
    for (raw, (job, entry)) in jobs.chain(iter::once(last)).zip(entries).enumerate() {
        let cpu = job.cpu;
        let domain = RANDOM_NAMESPACE.instance(raw as u64);
        let (task, parent) = Task::try_new(job, crate::random::derive_seed(&seed, domain), entry)
            .map_err(|source| execute::Cause::WorkerBus { cpu, source })?;
        values
            .try_push(task, parent)
            .map_err(execute::Cause::Allocation)?;
    }
    values.scope(move |values| {
        let mut running = Running {
            slots: Slots { values, failures },
        };
        if let Err((parent, source)) = running.slots.values.start(STACK) {
            let cpu = parent.cpu;
            return Err(running
                .stop()
                .fail(execute::Cause::WorkerSpawn { cpu, source }));
        }
        run(running)
    })
}

impl<'run, I, E, X> Running<'run, I, E, X> {
    pub(in crate::process::runtime) fn scope<R>(
        &self,
        apply: impl for<'id> FnOnce(bound::Bound<'id, I, E, X>) -> R,
    ) -> R {
        // SAFETY: Running retains every initial Main; only consuming stop removes one.
        apply(unsafe { bound::Bound::new(self.slots.values.entries()) })
    }

    pub(in crate::process::runtime) fn stop(self) -> Stopping<'run, I, E, X> {
        let Self { mut slots } = self;
        let mut stop = None;
        for parent in slots.values.parents_mut() {
            let source = parent.stop();
            if stop.is_none() {
                stop = source;
            }
        }
        Stopping { slots, stop }
    }
}

impl<'run, I, E, X> Stopping<'run, I, E, X> {
    pub(in crate::process::runtime) const fn can_observe(&self) -> bool {
        self.stop.is_none()
    }

    /// # Safety
    /// Every parent still owns its Main; Finish retains this admission.
    pub(in crate::process::runtime) unsafe fn scope<R>(
        &self,
        apply: impl for<'id> FnOnce(bound::Bound<'id, I, E, X>) -> R,
    ) -> R {
        // SAFETY: the caller retains Finish admission for all live Main owners.
        apply(unsafe { bound::Bound::new(self.slots.values.entries()) })
    }

    pub(in crate::process::runtime) fn clean<'r, S>(self) -> Result<(), execute::Error<'r, S, X>>
    where
        E: worker::Entry<I, X>,
    {
        let Self { slots, stop } = self;
        let (workers, missing) = slots.finish(None);
        execute::Error::finish(missing, stop, workers)
    }

    pub(in crate::process::runtime) fn fail<'r, S>(
        self,
        control: execute::Cause<S>,
    ) -> execute::Error<'r, S, X>
    where
        E: worker::Entry<I, X>,
    {
        let target = match &control {
            execute::Cause::WorkerFailed { cpu }
            | execute::Cause::BusRead {
                cpu,
                source: Read::Closed,
            } => Some(*cpu),
            _ => None,
        };
        let Self { slots, stop } = self;
        let (workers, missing) = slots.finish(target);
        execute::Error::failed(control, missing, stop, workers)
    }
}

impl<'run, I, E, X> Slots<'run, I, E, X> {
    fn finish(
        self,
        target: Option<affinity::Cpu>,
    ) -> (Vec<execute::Worker<X>>, Option<affinity::Cpu>)
    where
        E: worker::Entry<I, X>,
    {
        let Self {
            values: mut slots,
            mut failures,
        } = self;
        let mut missing = target;
        for parent in slots.parents_mut() {
            parent.handoff::<E>();
        }
        for (parent, output) in slots.finish() {
            let cpu = parent.cpu;
            let handoff = parent.handoff;
            let Output { outcome, terminal } = output;
            let failure = match outcome {
                worker::Outcome::Complete => match (terminal, handoff) {
                    (None, None) => continue,
                    (Some(source), handoff) => execute::Worker::Bus {
                        cpu,
                        source: execute::Bus::Write {
                            source,
                            terminal: None,
                            handoff,
                        },
                    },
                    (None, Some(source)) => execute::Worker::Bus {
                        cpu,
                        source: execute::Bus::Read(source),
                    },
                },
                worker::Outcome::Affinity { source, bus } => execute::Worker::Affinity {
                    cpu,
                    source,
                    bus,
                    terminal,
                    handoff,
                },
                worker::Outcome::Worker { source, bus } => execute::Worker::Entry {
                    cpu,
                    source,
                    bus,
                    terminal,
                    handoff,
                },
                worker::Outcome::Bus(source) => execute::Worker::Bus {
                    cpu,
                    source: execute::Bus::Write {
                        source,
                        terminal,
                        handoff,
                    },
                },
            };
            if target == Some(cpu) {
                missing = None;
            }
            // SAFETY: scope reserves one slot for each registered worker.
            // finish yields each started worker once, with at most one failure.
            unsafe { o3::alloc::raw::VecExt::vacancy(&mut failures) }.insert(failure);
        }
        (failures, missing)
    }
}
