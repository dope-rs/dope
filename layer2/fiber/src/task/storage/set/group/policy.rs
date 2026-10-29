use o3::depot::map;

use crate::core::driver::schedule::ready::task::owner;

/// Static grouping identity for one task metadata type.
pub trait Policy<M>: map::Hasher<Self::Key> + Sized {
    type Key: Copy + Eq + owner::Identity;

    fn key(task: &M) -> Self::Key;
}
