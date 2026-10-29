use dope_runtime::executor::{self, session::Manual};

fn erase<'app, 'life: 'app, 'd: 'life, 'owner: 'd, D, Q>(
    app: Manual<'app, 'life, 'd, D, Q>,
) -> Manual<'owner, 'owner, 'owner, D, Q>
where
    D: for<'brand> executor::raw::App<'brand>,
{
    app
}

fn main() {}
