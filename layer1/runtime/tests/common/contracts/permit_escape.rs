use dope_core::driver::storage;
use dope_runtime::{client::Provider, executor, executor::session, topology::lending};

fn escape<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, O>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    report: &mut session::Completion,
    owner: O,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d> + 'd,
    O::Provider: 'd,
{
    let _ = stage.lend_scope::<O, <O::Provider as Provider<'d>>::Client<'life>, _>(
        report,
        owner,
        |issued| {
            let (client, permit) = issued.into_parts();
            permit.reject(client)
        },
    );
}

fn main() {}
