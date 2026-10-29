use dope_runtime::topology::lending;

fn retag<'held, 'other: 'held, 'd: 'other, O>(
    held: lending::Held<'held, 'd, O>,
) -> lending::Held<'held, 'other, O>
where
    for<'driver> O: lending::Root<'driver> + lending::raw::Resume<'driver>,
{
    held
}

fn main() {}
