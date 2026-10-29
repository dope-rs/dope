use std::pin;

use o3::alloc;
use tree::Member as _;

use crate::task::storage::fixed::tree;

#[pin_project::pin_project(!Unpin)]
pub struct Branch<L, R> {
    #[pin]
    left: L,
    #[pin]
    right: R,
}

impl<L, R> Branch<L, R> {
    pub const fn new(left: L, right: R) -> Self {
        Self { left, right }
    }

    pub(crate) fn split(self: pin::Pin<&mut Self>) -> (pin::Pin<&mut L>, pin::Pin<&mut R>) {
        let branch = self.project();
        (branch.left, branch.right)
    }

    pub(crate) fn into_parts(self) -> (L, R) {
        (self.left, self.right)
    }
}

impl<L: Default, R: Default> Default for Branch<L, R> {
    fn default() -> Self {
        Self::new(L::default(), R::default())
    }
}

impl<'d, L, R> tree::Tree<'d> for Branch<L, R>
where
    L: tree::Tree<'d>,
    R: tree::Tree<'d>,
{
    const TASKS: tree::Tasks = L::TASKS.add(R::TASKS);
}

impl<'d, L, R> tree::Build<'d> for Branch<L, R>
where
    L: tree::Build<'d>,
    R: tree::Build<'d>,
{
    fn try_build() -> Result<Self, alloc::Error> {
        Ok(Self::new(L::try_build()?, R::try_build()?))
    }
}

impl<'d, L, R, P> tree::At<'d, tree::Left<P>> for Branch<L, R>
where
    L: tree::At<'d, P>,
    R: tree::Tree<'d>,
{
    type Member = L::Member;

    const OFFSET: tree::Offset = {
        let offset = L::OFFSET;
        assert!(L::TASKS.contains(offset, L::Member::CAPACITY));
        offset
    };

    fn project(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Member> {
        let branch = self.project();
        L::project(branch.left)
    }

    fn project_ref(&self) -> &Self::Member {
        let left = &self.left;
        L::project_ref(left)
    }
}

impl<'d, L, R, P> tree::At<'d, tree::Right<P>> for Branch<L, R>
where
    L: tree::Tree<'d>,
    R: tree::At<'d, P>,
{
    type Member = R::Member;

    const OFFSET: tree::Offset = R::OFFSET.shift(L::TASKS);

    fn project(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Member> {
        let branch = self.project();
        R::project(branch.right)
    }

    fn project_ref(&self) -> &Self::Member {
        let right = &self.right;
        R::project_ref(right)
    }
}
