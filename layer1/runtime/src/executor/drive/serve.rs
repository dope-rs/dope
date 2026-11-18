use std::ops::ControlFlow;

use dope_core::driver::schedule::clock::Sample;
use drive::{clock, turn};
use turn::scope;

use crate::executor::{self, drive};

pub(crate) fn run<'app, 'd: 'app, D, S, C>(
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
        match state.next_app(tick, &mut shutdown)?.park()? {
            ControlFlow::Break(()) => return Ok(()),
            ControlFlow::Continue(Some(idle)) => {
                idle.wait(|idle| idle.progress(), source)?;
            }
            ControlFlow::Continue(None) => {}
        }
    }
}
