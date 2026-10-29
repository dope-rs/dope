use crate::task::storage::{Handle, set::group};

/// An affine identity issued by one scoped task group.
#[must_use = "retain the task identity until completion or cancellation"]
pub struct Id<'s, 'd, D> {
    pub(in crate::task::storage) key: Handle<'d, group::Scope<'s, D>>,
}

const _: () = {
    assert!(size_of::<Id<'static, 'static, ()>>() == size_of::<u64>());
    assert!(size_of::<Option<Id<'static, 'static, ()>>>() == size_of::<u64>());
};
