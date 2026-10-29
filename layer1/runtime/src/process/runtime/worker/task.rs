use std::marker;

use dope_core::{
    platform::{affinity, affinity::Binding, channel, thread},
    random,
};

use crate::process::{
    self,
    failure::bus::{Event, Fault, Read, Report},
    layout,
    runtime::{bus, worker, worker::Entry},
};

pub(super) enum Outcome<X> {
    Complete,
    Affinity {
        source: affinity::Error,
        bus: Report,
    },
    Worker {
        source: X,
        bus: Report,
    },
    Bus(Fault),
}

fn execute<I, E, X>(
    job: layout::Assignment<I>,
    tx: bus::Tx<'_>,
    seed: random::Seed,
    entry: E,
) -> (Outcome<X>, Event)
where
    E: Entry<I, X>,
{
    let layout::Assignment { cpu, input } = job;
    let affinity = match Binding::bind(cpu) {
        Ok(affinity) => affinity,
        Err(error) => {
            return (
                Outcome::Affinity {
                    source: error,
                    bus: tx.report(),
                },
                Event::Failed,
            );
        }
    };
    let context = process::Context::new(affinity, seed, tx);
    match entry.run(input, context) {
        Ok(proof) => match proof.terminal(tx.report()) {
            Err(failure) => (Outcome::Bus(failure), Event::Failed),
            Ok(event) => (Outcome::Complete, event),
        },
        Err(error) => (
            Outcome::Worker {
                source: error,
                bus: tx.report(),
            },
            Event::Failed,
        ),
    }
}

pub(in crate::process::runtime) struct Task<I, E, X> {
    assignment: layout::Assignment<I>,
    peer: bus::Bus,
    seed: random::Seed,
    entry: E,
    output: marker::PhantomData<fn() -> X>,
}

pub(in crate::process::runtime) struct Parent {
    pub(in crate::process::runtime) main: Option<bus::Main>,
    pub(in crate::process::runtime) cpu: affinity::Cpu,
    pub(super) handoff: Option<Read>,
}

pub(in crate::process::runtime) struct Output<X> {
    pub(super) outcome: Outcome<X>,
    pub(super) terminal: Option<Fault>,
}

impl<I, E, X> Task<I, E, X> {
    pub(super) fn try_new(
        job: layout::Assignment<I>,
        seed: random::Seed,
        entry: E,
    ) -> Result<(Self, Parent), channel::Create> {
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
        let (outcome, event) = execute::<I, E, X>(job, peer.tx(), seed, entry);
        let terminal = peer.tx().send(event).err();
        Output { outcome, terminal }
    }
}

impl Parent {
    pub(super) fn stop(&mut self) -> Option<dope_core::Errno> {
        let source = self.main.as_ref()?.stop()?;
        self.main = None;
        self.handoff = Some(Read::Stopped(source));
        Some(source)
    }

    pub(super) fn handoff<M: bus::Mode>(&mut self) {
        if let Some(main) = self.main.take() {
            self.handoff = main.handoff::<M>();
        }
    }
}
