use dope_core::driver::storage;
use dope_runtime::{executor, executor::session, topology::lending};

fn replay<'held, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p + 'held, S, Q, O>(
    held: lending::Held<'held, 'd, O>,
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    report: &mut session::Completion,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    let _ = held.lend_scope(stage, report, |issued| issued.into_parts().1.reject(()));
    let _ = held.lend_scope(stage, report, |issued| issued.into_parts().1.reject(()));
}

fn main() {}
