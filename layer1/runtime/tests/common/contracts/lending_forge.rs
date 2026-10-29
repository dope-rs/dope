fn main() {
    use dope_runtime::topology::lending;
    lending::raw::Bound::<(), ()>::new(loop {}, ());
}
