use clock::{Bound, End, Expiring as _, Time as _};
use dope_core::driver::schedule;
use drive::{clock, turn};
use turn::scope;

use crate::executor::{drain, drive, raw};

pub(crate) fn run<'app, 'd: 'app, D, S, C>(
    mut state: scope::Scope<'_, '_, 'app, 'd, D, S>,
    source: Bound<C, End>,
    mut shutdown: impl FnMut() -> turn::Status,
) -> Result<drain::Outcome, drive::Error>
where
    D: raw::Drain<'d>,
    C: clock::Source,
{
    let tick = source.start();
    let ready = state.next_app(tick, &mut shutdown)?;
    ready.begin_drain();
    loop {
        let tick = match source.tick() {
            Ok(tick) => tick,
            Err(clock::Elapsed) => {
                let tick = source.start();
                let ready = state.next_app(tick, &mut shutdown)?;
                ready.force_drain();
                return Ok(drain::Outcome::Forced);
            }
        };
        let Some(idle) = state.next_app(tick, &mut shutdown)?.park().sync()? else {
            continue;
        };
        let drain = idle.drain();
        if idle.is_quiet() && matches!(drain, schedule::Progress::Quiescent) {
            return Ok(drain::Outcome::Quiesced);
        }
        let progress = idle.progress().reduce(drain);
        idle.wait(progress, source)?;
    }
}
