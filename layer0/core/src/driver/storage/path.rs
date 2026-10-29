use std::mem;

use crate::driver::{storage, storage::Path};

/// The path from a storage plan to itself.
#[derive(Clone, Copy, Default)]
pub struct Here;

impl<S: storage::Plan> Path<S, S> for Here {
    fn get<'life, 'p, 'd>(self, root: &'life S::Output<'p, 'd>) -> &'life S::Output<'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
    {
        root
    }
}

const _: () = {
    assert!(mem::size_of::<Here>() == 0);
    assert!(mem::align_of::<Here>() == 1);
    assert!(!mem::needs_drop::<Here>());
};
