use std::marker;

use dope_core::{
    platform::{affinity, affinity::Binding, thread},
    random,
};

use crate::process::{
    self,
    failure::bus::{Create, Event, Read},
    layout,
    runtime::{bus, worker, worker::Entry},
};

pub(in crate::process::runtime) enum Failure<X> {
    Affinity(affinity::Error),
    Entry(X),
}

fn execute<I, E, X>(
    job: layout::Assignment<I>,
    tx: bus::Tx<'_>,
    seed: random::Seed,
    entry: E,
) -> Result<(), Failure<X>>
where
    E: Entry<I, X>,
{
    let layout::Assignment { cpu, input } = job;
    let affinity = match Binding::bind(cpu) {
        Ok(affinity) => affinity,
        Err(error) => {
            let _ = tx.send(Event::Failed);
            return Err(Failure::Affinity(error));
        }
    };
    let context = process::Context::new(affinity, seed, tx);
    entry.run(input, context).map(drop).map_err(Failure::Entry)
}

pub(in crate::process::runtime) struct Task<I, E, X> {
    assignment: layout::Assignment<I>,
    peer: bus::Worker,
    seed: random::Seed,
    entry: E,
    output: marker::PhantomData<fn() -> X>,
}

pub(in crate::process::runtime) struct Parent {
    pub(in crate::process::runtime) main: Option<bus::Main>,
    pub(in crate::process::runtime) cpu: affinity::Cpu,
    pub(super) handoff: Option<Read>,
}

pub(in crate::process::runtime) type Output<X> = bus::Outcome<(), Failure<X>>;

impl<I, E, X> Task<I, E, X> {
    pub(super) fn try_new(
        job: layout::Assignment<I>,
        seed: random::Seed,
        entry: E,
    ) -> Result<(Self, Parent), Create> {
        let cpu = job.cpu;
        let (main, peer) = bus::Main::try_pair()?;
        Ok((
            Self {
                assignment: job,
                peer,
                seed,
                entry,
                output: marker::PhantomData,
            },
            Parent {
                main: Some(main),
                cpu,
                handoff: None,
            },
        ))
    }
}

impl<I: Send, E: worker::Entry<I, X>, X: Send> thread::Task for Task<I, E, X> {
    type Output = Output<X>;
    fn run(self) -> Self::Output {
        let Self {
            assignment: job,
            peer,
            seed,
            entry,
            output: _,
        } = self;
        peer.run(|shutdown| execute::<I, E, X>(job, shutdown.source, seed, entry))
    }
}

impl Parent {
    pub(super) fn stop(&mut self) -> Option<dope_core::Errno> {
        match self.main.take()?.stop() {
            Ok(main) => {
                self.main = Some(main);
                None
            }
            Err(source) => {
                self.handoff = Some(Read::Stopped(source));
                Some(source)
            }
        }
    }

    pub(super) fn handoff<M: bus::Mode>(&mut self, readiness: bus::Readiness) {
        if let Some(main) = self.main.take() {
            self.handoff = main.handoff::<M>(readiness);
        }
    }
}
