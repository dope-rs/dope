use dope_runtime::executor;

fn forward<'app, 'd, T: executor::raw::App<'d>>(
    released: executor::raw::Released<'app, T>,
) -> executor::raw::Released<'app, T> {
    released.visit(|app| app)
}

fn main() {}
