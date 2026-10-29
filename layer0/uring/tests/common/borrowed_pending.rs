use dope_sys::memory::{self, buffer::provided};
use dope_uring::Uring;

fn duplicate<E: memory::raw::Extent + Copy>(token: <Uring as provided::Source>::Pending<E>) {
    let first = token;
    drop((first, token));
}
