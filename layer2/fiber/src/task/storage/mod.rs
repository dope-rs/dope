pub(crate) mod caps;
pub mod set;

pub mod fixed;
pub mod phase;
pub mod scope;
pub(crate) mod slots;

use o3::depot::arena::slab::key;
pub use scope::Id;
pub(crate) use scope::Owner;

/// The native key's invariant tag preserves the driver and task domain.
type Handle<'d, D> = key::Handle<(&'d mut &'d (), D)>;

const _: () = {
    assert!(size_of::<Handle<'static, ()>>() == size_of::<u64>());
    assert!(size_of::<Option<Handle<'static, ()>>>() == size_of::<u64>());
};
