use crate::{memory, memory::buffer};

/// Selects native receive ownership while retaining the caller's layout proof.
pub trait Source {
    type Pool<L: memory::raw::Layout>: buffer::Pool<Layout = L>;
}
