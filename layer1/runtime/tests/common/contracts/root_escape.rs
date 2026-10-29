use std::pin::Pin;

use dope_runtime::executor::root;

fn escape<'a, 'd: 'a, R>(value: root::Context<'a, 'd, R>) -> Pin<&'d mut R> {
    value.into_parts().0
}

fn main() {}
