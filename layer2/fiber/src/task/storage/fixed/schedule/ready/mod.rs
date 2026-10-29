use std::{marker, num::NonZero, pin};

use crate::{
    abi,
    context::wake,
    core::driver::schedule::ready::{
        task,
        task::{Error, Parent, topology},
    },
    task::storage::{
        caps,
        fixed::{schedule, schedule::selected, tree},
    },
};

pub mod linked;
pub mod owned;
pub(in crate::task::storage::fixed::schedule) mod raw;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Global(NonZero<u32>);

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
    pub(in crate::task::storage::fixed::schedule) fn local<'d, const N: usize>(
        self,
        _: caps::Tasks<N>,
    ) -> raw::Local<N>
    where
        M: tree::Member<'d>,
    {
        const { assert!(M::CAPACITY.nonzero().get() == caps::Tasks::<N>::new().nonzero().get()) };
        // SAFETY: the resident index is within the member capacity proved equal to N.
        unsafe { raw::Local::from_raw(self.0.index.get() as u32) }
    }

    pub(in crate::task::storage::fixed::schedule) fn root<'d, R, P>(
        self,
        _: caps::Path<R, P>,
    ) -> TreeIndex<R>
    where
        R: tree::At<'d, P, Member = M>,
    {
        // SAFETY: the resident is below its member capacity and Path proves
        // offset + capacity <= the root's nonzero u32 task count.
        let index = unsafe { NonZero::new_unchecked(self.0.index.0.get() - R::OFFSET.into_u32()) };
        TreeIndex {
            index: Global(index),
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
    pub(in crate::task::storage::fixed::schedule) fn split<'d>(
        self,
    ) -> Result<TreeIndex<L>, TreeIndex<R>>
    where
        L: tree::Tree<'d>,
        R: tree::Tree<'d>,
    {
        let tasks = L::TASKS.nonzero().get();
        if self.raw() < tasks {
            Ok(TreeIndex {
                index: self.index,
                tree: marker::PhantomData,
            })
        } else {
            // SAFETY: this branch proves local >= tasks, so adding tasks to
            // its nonzero complement cannot overflow or produce zero.
            let index = unsafe { NonZero::new_unchecked(self.index.0.get() + tasks) };
            Err(TreeIndex {
                index: Global(index),
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
    proof: caps::Path<R, P>,
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
            proof: caps::Path::<R, P>::new(),
            driver: marker::PhantomData,
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn split(
        self,
    ) -> (pin::Pin<&'a mut R::Member>, caps::Path<R, P>) {
        (self.member, self.proof)
    }

    pub(in crate::task::storage::fixed::schedule) fn index<const N: usize>(
        proof: caps::Path<R, P>,
        local: raw::Local<N>,
    ) -> Global {
        let offset = proof.local::<N>();
        // SAFETY: Path proves offset + N <= u32::MAX and raw::Local proves local < N,
        // so subtracting offset from the complemented local index stays nonzero.
        Global(unsafe { NonZero::new_unchecked(!local.raw() - offset.into_u32()) })
    }
}

impl Global {
    pub(in crate::task::storage) const fn new(position: NonZero<u32>) -> Self {
        Self(position.cast_signed().wrapping_neg().cast_unsigned())
    }

    pub(in crate::task::storage) const fn position(self) -> NonZero<u32> {
        self.0.cast_signed().wrapping_neg().cast_unsigned()
    }

    pub(in crate::task::storage) const fn get(self) -> usize {
        (!self.0.get()) as usize
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
    assert!(mem::size_of::<raw::Local<1>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<raw::Local<1>>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Global>>() == mem::size_of::<Global>());
    assert!(mem::size_of::<Resident<Tree<'_>>>() == mem::size_of::<u32>());
    assert!(!mem::needs_drop::<Resident<Tree<'_>>>());
    assert!(mem::size_of::<topology::Endpoint<'static, Global>>() == 2 * word);
    assert!(mem::size_of::<topology::Slot<'static, Global>>() == 2 * word);
    assert!(mem::size_of::<topology::Edge<'static, 'static, Global>>() == 2 * word);
};
