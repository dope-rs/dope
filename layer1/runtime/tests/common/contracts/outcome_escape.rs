use dope_core::driver::storage;
use dope_runtime::{executor, executor::session, topology::lending};

fn replace<'scope, 'outer, 'life: 'scope + 'outer, 'p: 'life, 'd: 'p, S, Q, O, E>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    report: &mut session::Completion,
    owner: O,
    foreign: lending::Outcome<'outer, 'life, 'd, O, Q, E>,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d> + 'd,
    O::Provider: 'd,
{
    let _ = stage.lend_scope::<O, E, _>(report, owner, |issued| {
        let (_, permit) = issued.into_parts();
        drop(permit);
        foreign
    });
}

fn main() {}
