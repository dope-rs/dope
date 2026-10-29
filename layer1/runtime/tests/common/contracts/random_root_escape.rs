use dope_runtime::random::{Purpose, Root};

fn widen<'d, 'owner: 'd, P: Purpose>(root: Root<'d, P>) -> Root<'owner, P> {
    root
}

fn main() {}
