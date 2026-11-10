mod plane;
mod pool;
pub mod queue;
pub mod ready;
pub mod result;
pub mod selected;

pub use pool::{Admission, Mode, Pool, Storage, Stream};

pub type Id<'s, 'd, D> = crate::task::storage::fixed::Id<
    's,
    'd,
    D,
    o3::depot::arena::slab::external::Key<o3::base::num::Epoch>,
>;
