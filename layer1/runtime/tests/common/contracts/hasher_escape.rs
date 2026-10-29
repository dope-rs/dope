use std::hash::BuildHasher;

use dope_runtime::random::{HashState, Hasher};

fn erase<'d, 'owner: 'd>() {
    let _ = |state: HashState<'d>| -> Hasher<'owner> { state.build_hasher() };
}

fn main() {}
