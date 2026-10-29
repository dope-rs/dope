use clock::{Bound, End, Expiring as _, Time as _};
use dope_core::driver::schedule;
use drive::{clock, turn};
use turn::scope;

use crate::executor::{drain, drive, raw};

pub(crate) struct Run<'run, 'driver, 'app, 'd: 'app, D: raw::Layout<'d>, S, C> {
    state: scope::Scope<'run, 'driver, 'app, 'd, D, S>,
    source: Bound<C, End>,
}

impl<'run, 'driver, 'app, 'd: 'app, D, S, C> Run<'run, 'driver, 'app, 'd, D, S, C>
where
    D: raw::Drain<'d>,
    C: clock::Source,
{
    pub(crate) const fn new(
        state: scope::Scope<'run, 'driver, 'app, 'd, D, S>,
        source: Bound<C, End>,
    ) -> Self {
        Self { state, source }
    }

    pub(crate) fn run(self) -> Result<drain::Outcome, drive::Error> {
        let mut this = self.begin()?;
        loop {
            let tick = match this.source.tick() {
                Ok(tick) => tick,
                Err(clock::Elapsed) => return this.force(),
            };
            let parked = this.state.next_app(tick)?.skip().park();
            let quiesced = parked.resolve(this.source, |idle| {
                if idle.is_quiet() && matches!(idle.drain(), schedule::Progress::Quiescent) {
                    return Ok(turn::Resolved::Done);
                }
                let state = idle.progress();
                Ok(turn::Resolved::Wait(idle, state))
            })?;
            if quiesced {
                return Ok(drain::Outcome::Quiesced);
            }
        }
    }

    fn begin(mut self) -> Result<Self, drive::Error> {
        let tick = self.source.start();
        let ready = self.state.next_app(tick)?;
        ready.begin_drain();
        Ok(self)
    }

    fn force(mut self) -> Result<drain::Outcome, drive::Error> {
        let tick = self.source.start();
        let ready = self.state.next_app(tick)?;
        ready.force_drain();
        Ok(drain::Outcome::Forced)
    }
}
