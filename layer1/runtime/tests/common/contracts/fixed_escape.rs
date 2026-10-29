use dope_runtime::executor::session::Fixed;

fn erase<'d, 'owner: 'd>(fixed: Fixed<'d, ()>) -> Fixed<'owner, ()> {
    fixed
}

fn main() {}
