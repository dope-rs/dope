use dope_runtime::executor::root;

fn retag<'a, 'd: 'a, A, B>(value: root::Context<'a, 'd, A>) -> root::Context<'a, 'd, B> {
    value
}

fn main() {}
