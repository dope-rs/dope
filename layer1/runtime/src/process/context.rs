use dope_core::{
    driver::{life::creation, settings},
    platform::{affinity, affinity::Binding},
    random,
};

use crate::{
    executor::{self, create},
    process::{self, gate, runtime::bus},
};

pub struct Context<'run> {
    affinity: Binding,
    seed: random::Seed,
    shutdown: process::Shutdown<'run>,
}

impl<'run> Context<'run> {
    pub(super) const fn new(bind: Binding, seed: random::Seed, tx: bus::Tx<'run>) -> Self {
        Self {
            affinity: bind,
            seed,
            shutdown: process::Shutdown::new(tx),
        }
    }

    pub fn cpu(&self) -> affinity::Cpu {
        self.affinity.cpu()
    }

    pub fn terminal(&self) -> process::Terminal<'run> {
        process::Terminal {
            source: self.shutdown.source,
        }
    }

    pub(super) fn gated(self) -> (Self, gate::Closed<'run>) {
        let gate = self.shutdown.gate();
        (self, gate)
    }
}

impl<'run> executor::Build for Context<'run> {
    type Shutdown = process::Shutdown<'run>;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<executor::Executor<(), Self::Shutdown>, create::Error<'_>> {
        executor::Executor::with_seed(attempt, config, self.seed)?
            .shutdown(self.shutdown)
            .map_err(create::Error::from)
    }
}
