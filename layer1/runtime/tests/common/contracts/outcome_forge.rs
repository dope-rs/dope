use dope_runtime::topology::lending;

fn forge<'issued, 'life: 'issued, 'd: 'life, O, Q>()
where
    O: lending::raw::Resume<'d>,
{
    let _ = lending::Outcome::<'issued, 'life, 'd, O, Q, ()>::executed(loop {}, loop {}, loop {});
}

fn main() {}
