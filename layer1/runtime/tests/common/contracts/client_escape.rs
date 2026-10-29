use dope_runtime::client::Bound;
fn widen<'a, 'b: 'a, 'd: 'b, P, T>(value: Bound<'a, 'd, P, T>) -> Bound<'b, 'd, P, T> {
    value
}

fn main() {}
