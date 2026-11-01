use dope_core::{driver::route, io::transfer::Len};
use dope_net::wire::{
    Identity,
    send::{self, reclaim},
};

fn replay_storage<'a, 'd: 'a, ID: route::Identity + 'd>(
    context: send::Context<'a, 'd, ID, Identity, send::Storage<'a, ()>>,
) -> (Len, Len) {
    let storage = context.into_state();
    let first = storage.empty::<reclaim::OnSubmit>();
    let second = storage.empty::<reclaim::OnSubmit>();
    (first.into_parts().1, second.into_parts().1)
}

fn borrowed_storage<'a, 'd: 'a, ID: route::Identity + 'd>(
    context: send::Context<'a, 'd, ID, Identity, send::Storage<'a, ()>>,
) -> (Len, usize) {
    let storage = context.into_state();
    let bytes = storage.as_slice();
    let prepared = storage.empty::<reclaim::OnSubmit>();
    (prepared.into_parts().1, bytes.len())
}

fn main() {}
