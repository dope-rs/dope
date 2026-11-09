use std::{marker, num::NonZero, pin};

use o3::depot::queue::links;

use crate::{
    abi,
    context::wake,
    core::driver::schedule::ready::{
        task,
        task::{Error, Parent, topology},
    },
    task::storage::fixed::{schedule, schedule::selected, tree},
};

pub mod linked;
pub mod owned;
pub(in crate::task::storage::fixed::schedule) mod raw;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Global(links::Index);

#[repr(transparent)]
pub struct TreeIndex<R> {
    index: Global,
    tree: marker::PhantomData<fn(R) -> R>,
}

#[repr(transparent)]
pub struct Resident<M>(TreeIndex<tree::Leaf<M>>);

impl<M> Copy for Resident<M> {}
impl<M> Clone for Resident<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Resident<M> {
    pub(in crate::task::storage::fixed::schedule) fn local(self, base: u32) -> u32 {
        self.0.raw() - base
    }

    pub(in crate::task::storage::fixed::schedule) fn root<'d, R, P>(self) -> TreeIndex<R>
    where
        R: tree::At<'d, P, Member = M>,
    {
        TreeIndex {
            index: self.0.index,
            tree: marker::PhantomData,
        }
    }
}

impl<M> TreeIndex<tree::Leaf<M>> {
    pub(in crate::task::storage::fixed::schedule) const fn resident(self) -> Resident<M> {
        Resident(self)
    }
}

impl<R> Copy for TreeIndex<R> {}
impl<R> Clone for TreeIndex<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, R: schedule::PlaneTree<'d>> selected::Index<'d, R> for TreeIndex<R> {
    fn into_root(self) -> Self {
        self
    }
}

impl<R> TreeIndex<R> {
    pub(in crate::task::storage::fixed::schedule) const fn raw(self) -> u32 {
        self.index.get() as u32
    }

    pub(in crate::task::storage::fixed::schedule) const fn global(self) -> Global {
        self.index
    }
}

impl<L, R> TreeIndex<tree::Branch<L, R>> {
    pub(in crate::task::storage::fixed::schedule) fn split(
        self,
        right: u32,
    ) -> Result<TreeIndex<L>, TreeIndex<R>> {
        if self.raw() < right {
            Ok(TreeIndex {
                index: self.index,
                tree: marker::PhantomData,
            })
        } else {
            Err(TreeIndex {
                index: self.index,
                tree: marker::PhantomData,
            })
        }
    }
}

#[repr(transparent)]
pub struct Path<'a, 'd, R, P>
where
    R: tree::At<'d, P>,
{
    member: pin::Pin<&'a mut R::Member>,
    tree: marker::PhantomData<fn(R) -> R>,
    path: marker::PhantomData<fn(P) -> P>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'a, 'd, R, P> Path<'a, 'd, R, P>
where
    R: tree::At<'d, P>,
{
    pub(in crate::task::storage::fixed::schedule) fn new(
        member: pin::Pin<&'a mut R::Member>,
    ) -> Self {
        Self {
            member,
            tree: marker::PhantomData,
            path: marker::PhantomData,
            driver: marker::PhantomData,
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn split(self) -> pin::Pin<&'a mut R::Member> {
        self.member
    }
}

impl Global {
    pub(in crate::task::storage) fn new(position: NonZero<u32>) -> Self {
        Self(position.into())
    }

    pub(in crate::task::storage) const fn get(self) -> usize {
        self.0.get()
    }
}

pub(in crate::task::storage::fixed::schedule) fn try_bind<'a, 'd, I, P, E>(
    parent: wake::RootWaker<'d>,
    prepare: impl FnOnce(Parent<'d>) -> Result<(topology::Edge<'a, 'd, I>, P), E>,
) -> Result<(task::raw::Root<'d>, P), E>
where
    'd: 'a,
    I: topology::raw::Index + 'd,
    E: From<Error>,
{
    let parent: wake::Waker<'d> = parent.into();
    let admit = parent.0.try_admit().map_err(E::from)?;
    let (edge, prepared) = prepare(parent.0)?;
    let binding = unsafe { task::raw::Root::bind(admit, edge) };
    Ok((binding, prepared))
}

const _: () = {
    use core::mem;

    let word = mem::size_of::<usize>();
    type Tree<'d> = tree::Leaf<schedule::Pool<'d, abi::Ready<()>, 1, ()>>;
    type Proof<'a, 'd> = self::Path<'a, 'd, Tree<'d>, tree::Here>;
    assert!(mem::size_of::<Proof<'_, '_>>() == word);
    assert!(mem::align_of::<Proof<'_, '_>>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Proof<'_, '_>>());
    assert!(mem::size_of::<Global>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Global>>() == mem::size_of::<Global>());
    assert!(mem::size_of::<Resident<Tree<'_>>>() == mem::size_of::<u32>());
    assert!(!mem::needs_drop::<Resident<Tree<'_>>>());
    assert!(mem::size_of::<topology::Endpoint<'static, Global>>() == 2 * word);
    assert!(mem::size_of::<topology::Slot<'static, Global>>() == 2 * word);
    assert!(mem::size_of::<topology::Edge<'static, 'static, Global>>() == 2 * word);
};
