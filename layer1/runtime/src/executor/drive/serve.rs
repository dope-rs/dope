use dope_core::driver::schedule::clock::Sample;
use drive::{clock, turn};
use turn::scope;

use crate::executor::{self, drive};

pub(crate) fn run<'app, 'd: 'app, D, S, C>(
    mut state: scope::Scope<'_, '_, 'app, 'd, D, S>,
    source: C,
    tick: impl Fn(C) -> Result<Sample, drive::Error>,
) -> Result<(), drive::Error>
where
    D: executor::raw::App<'d>,
    C: clock::Time,
{
    loop {
        let tick = tick(source)?;
        let parked = state.next_app(tick)?.skip().park();
        match parked.status() {
            turn::Status::Active => {}
            turn::Status::Shutdown => return Ok(()),
        }
        parked.resolve(source, |idle| {
            let state = idle.progress();
            Ok(turn::Resolved::Wait(idle, state))
        })?;
    }
}
