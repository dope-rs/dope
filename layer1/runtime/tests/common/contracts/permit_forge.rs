use dope_runtime::{executor, topology::lending};

fn forge<'issued, 'life: 'issued, 'd: 'life, O, Q>()
where
    O: lending::Root<'d> + lending::raw::Resume<'d>,
    O::Provider: 'issued,
    Q: executor::Startup,
{
    let _ = lending::Permit::<'issued, 'life, 'd, O, Q>::new(loop {}, loop {}, loop {});
}

fn main() {}
