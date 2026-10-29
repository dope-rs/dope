use dope_runtime::executor::{self, session::App};
fn run_twice<'app, 'life: 'app, 'd: 'life, D: executor::raw::App<'d>, Q>(
    app: App<'app, 'life, 'd, D, Q>,
) {
    app.run();
    app.run();
}

fn main() {}
