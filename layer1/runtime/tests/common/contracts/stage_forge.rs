use dope_core::driver::storage;
use dope_runtime::{executor::session::Scope, topology::Stage};

fn forge<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q>(
    session: &'stage mut Scope<'scope, 'life, 'p, 'd, S, Q>,
) -> Stage<'stage, 'scope, 'life, 'p, 'd, S, Q> {
    Stage::new(session)
}

fn main() {}
