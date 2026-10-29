use std::{convert, mem};

use dope_core::driver::storage;

use crate::topology::recipe;

/// Borrows the complete output of one storage plan.
#[derive(Clone, Copy, Default)]
pub struct Borrow;

impl<S> recipe::Build<S> for Borrow
where
    S: storage::Plan,
{
    type Output<'life, 'p, 'd>
        = &'life S::Output<'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
    type Error = convert::Infallible;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
    {
        Ok(storage)
    }
}

const _: () = {
    assert!(mem::size_of::<Borrow>() == 0);
    assert!(mem::align_of::<Borrow>() == 1);
    assert!(!mem::needs_drop::<Borrow>());
};
