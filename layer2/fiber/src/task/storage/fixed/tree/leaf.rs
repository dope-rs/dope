use std::pin;

use crate::task::storage::fixed::tree;

#[repr(transparent)]
#[pin_project::pin_project(!Unpin)]
pub struct Leaf<S> {
    #[pin]
    storage: S,
}

impl<S> Leaf<S> {
    pub const fn new(storage: S) -> Self {
        Self { storage }
    }

    pub(crate) fn storage(self: pin::Pin<&mut Self>) -> pin::Pin<&mut S> {
        self.project().storage
    }

    pub(crate) fn into_inner(self) -> S {
        self.storage
    }
}

impl<S: Default> Default for Leaf<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<'d, S> tree::Tree<'d> for Leaf<S>
where
    S: tree::Member<'d>,
{
    const TASKS: tree::Tasks = tree::Tasks::from(S::CAPACITY);
}

impl<'d, S> tree::At<'d, tree::Here> for Leaf<S>
where
    S: tree::Member<'d>,
{
    type Member = S;

    const OFFSET: tree::Offset = tree::Offset::new(0);

    fn project(self: pin::Pin<&mut Self>) -> pin::Pin<&mut S> {
        self.project().storage
    }

    fn project_ref(&self) -> &S {
        &self.storage
    }
}
