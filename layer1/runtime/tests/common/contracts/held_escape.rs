use dope_core::driver::storage;
use dope_runtime::{executor, topology::lending};

fn escape<'outside, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p + 'outside, S, Q, O>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    owner: O,
) -> lending::Held<'outside, 'd, O>
where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    stage.hold(owner, |held, _| held)
}

fn main() {}
