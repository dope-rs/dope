use std::mem;

use dope_sys::{memory, probe, ring, table::token};
use dope_uring::Uring;

fn detach<T: token::Codec, L: memory::raw::Canary>(
    state: &mut <Uring as probe::Source>::Session<T, L>,
    next: <Uring as ring::Source>::Ring<T>,
) {
    let _ = mem::replace(state.ring(), next);
}
