use dope_runtime::topology::lending;

fn forge<'held, 'd: 'held, O>()
where
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    let _ = lending::Held::<'held, 'd, O>::new(loop {});
}

fn main() {}
