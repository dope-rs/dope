use dope_core::driver::{result::poll, schedule};
use drive::{clock, turn};
use schedule::clock::Sample;
use turn::scope;

use crate::executor::{self, drive};

pub(super) fn run<'app, 'd: 'app, D, S, C>(
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
        let ready = state.next_app(tick)?;
        if ready.is_quiet() && matches!(ready.shutdown(), schedule::Progress::Quiescent) {
            match ready.sync()? {
                poll::Commit::Drained => return Ok(()),
                poll::Commit::Pending => continue,
            }
        }
        let parked = ready.skip().park();
        let quiesced = parked.resolve(source, |idle| {
            let state = idle.shutdown();
            if idle.is_quiet() && matches!(state, schedule::Progress::Quiescent) {
                return Ok(match idle.sync()? {
                    poll::Commit::Drained => turn::Resolved::Done,
                    poll::Commit::Pending => turn::Resolved::Again,
                });
            }
            Ok(turn::Resolved::Wait(idle, state))
        })?;
        if quiesced {
            return Ok(());
        }
    }
}
