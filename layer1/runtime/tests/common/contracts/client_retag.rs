use dope_runtime::client::Bound;
fn retag<'a, 'd: 'a, P, Q>(value: Bound<'a, 'd, P>) -> Bound<'a, 'd, Q> {
    value
}

fn main() {}
