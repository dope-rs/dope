use dope_runtime::{executor, topology::lending};

fn replay<'issued, 'life: 'issued, 'd: 'life, O, Q, A>(
    permit: lending::Permit<'issued, 'life, 'd, O, Q>,
    first: A,
    second: A,
) where
    O: lending::Root<'d> + lending::raw::Resume<'d>,
    O::Provider: 'issued,
    Q: executor::Startup,
    A: executor::raw::App<'d>,
{
    let _ = permit.execute::<A, ()>(first);
    let _ = permit.execute::<A, ()>(second);
}

fn main() {}
