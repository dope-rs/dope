use dope_core::driver::{result::poll, schedule};
use drive::{clock, turn};
use schedule::clock::Sample;
use turn::scope;

use crate::executor::{self, drive};

pub(super) fn run<'app, 'd: 'app, D, S, C>(
    mut state: scope::Scope<'_, '_, 'app, 'd, D, S>,
    source: C,
    tick: impl Fn(C) -> Result<Sample, drive::Error>,
    mut shutdown: impl FnMut() -> turn::Status,
) -> Result<(), drive::Error>
where
    D: executor::raw::App<'d>,
    C: clock::Time,
{
    loop {
        let tick = tick(source)?;
        let ready = state.next_app(tick, &mut shutdown)?;
        if ready.is_quiet() && matches!(ready.shutdown(), schedule::Progress::Quiescent) {
            match ready.sync()? {
                poll::Commit::Drained => return Ok(()),
                poll::Commit::Pending => continue,
            }
        }
        let Some(idle) = ready.skip().park().sync()? else {
            continue;
        };
        let progress = idle.shutdown();
        if idle.is_quiet() && matches!(progress, schedule::Progress::Quiescent) {
            match idle.sync()? {
                poll::Commit::Drained => return Ok(()),
                poll::Commit::Pending => continue,
            }
        }
        idle.wait(progress, source)?;
    }
}
