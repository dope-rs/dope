use std::pin;

use o3::{
    base::num::Epoch,
    depot::{arena::slab::external, capacity},
};
use ready::{Global, Path};
use storage::fixed::{local::Removal, tree};

use crate::{
    context::wake,
    core::driver::{
        self,
        schedule::{
            app::{self, step},
            ready::task,
        },
    },
    task::storage,
};

mod plan;
mod plane;
mod pool;
pub mod queue;
pub mod ready;
pub mod result;
mod route;
pub mod selected;

pub use plan::Plan;
pub use plane::Plane;
pub use pool::{Pool, Stream};
pub use route::Route;

pub type Id<'d, D> = storage::Id<'d, D, external::Key<Epoch>>;
pub(in crate::task::storage) type Scoped<'s, 'd, D> =
    storage::scope::Scoped<'s, 'd, D, external::Key<Epoch>>;

pub(in crate::task::storage) type Input<'d, M> = (
    <M as tree::Member<'d>>::Fiber,
    <M as tree::Member<'d>>::Target,
);

type Identity<'d, M> = Id<'d, <M as tree::Member<'d>>::Domain>;

/// Operations supplied by one fixed scheduler leaf.
pub trait Ops<'d>: tree::Member<'d> + pool::Proof + Sized {
    type Output;
    type Item;
    fn target_index(entry: selected::EntryRef<'_, Self>) -> Self::Target;

    fn try_admit<'a, S, R, P>(
        path: Path<'a, 'd, R, P>,
        queue: pin::Pin<&'a S>,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(Input<'d, Self>) -> Identity<'d, Self>, S::Error>
    where
        S: queue::Backend<'d>,
        R: tree::At<'d, P, Member = Self>;

    fn try_spawn<'a, R, P>(
        path: Path<'a, 'd, R, P>,
        queue: pin::Pin<&'a queue::Flat<'d>>,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(Input<'d, Self>) + 'a, task::Error>
    where
        R: tree::At<'d, P, Member = Self> + 'a,
        P: 'a;

    fn drive_index<'turn, 'queue, S>(
        entry: selected::Entry<'_, Self>,
        dequeued: S::Dequeued<'queue, Global>,
        work: app::Work<'turn, 'd>,
        permit: step::Delegated<'_, 'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> result::Outcome<Self::Target, Self::Output, Self::Item>
    where
        S: queue::Backend<'d> + 'queue;

    fn remove<'a, S, R, P>(
        path: Path<'a, 'd, R, P>,
        id: Identity<'d, Self>,
        queue: pin::Pin<&S>,
    ) -> Removal
    where
        S: queue::Backend<'d>,
        R: tree::At<'d, P, Member = Self>;
}

/// Access to a yielded stream while its original slot remains occupied.
pub trait StreamOps<'d>: Ops<'d> {
    fn get_mut(
        self: pin::Pin<&mut Self>,
        id: &Identity<'d, Self>,
    ) -> Result<pin::Pin<&mut Self::Fiber>, result::Error>;

    fn resume(self: pin::Pin<&mut Self>, id: &Identity<'d, Self>) -> Result<(), result::Error>;
}

/// Recursive operations over a fixed scheduler tree.
pub trait PlaneTree<'d>: tree::Build<'d> {
    fn layout(&mut self, base: u32) -> capacity::Count;

    fn first_base(&self) -> u32;

    fn root_waker(access: selected::Access<'_, Self>) -> wake::RootWaker<'d>;

    fn merge_ready(access: selected::Access<'_, Self>, queue: pin::Pin<&queue::Flat<'d>>);

    fn restore<'queue, S>(
        access: selected::Access<'_, Self>,
        dequeued: S::Dequeued<'queue, Global>,
    ) where
        S: queue::Backend<'d> + 'queue;
}

impl<'d, L, R> PlaneTree<'d> for tree::Branch<L, R>
where
    L: PlaneTree<'d>,
    R: PlaneTree<'d>,
{
    fn layout(&mut self, base: u32) -> capacity::Count {
        let (left, right) = self.parts_mut();
        let left = left.layout(base).get() as u32;
        let right = right.layout(base.wrapping_add(left)).get() as u32;
        capacity::Count::new(left.wrapping_add(right))
    }

    fn first_base(&self) -> u32 {
        self.left().first_base()
    }

    fn root_waker(access: selected::Access<'_, Self>) -> wake::RootWaker<'d> {
        match access.split() {
            Ok(access) => L::root_waker(access),
            Err(access) => R::root_waker(access),
        }
    }

    fn merge_ready(access: selected::Access<'_, Self>, queue: pin::Pin<&queue::Flat<'d>>) {
        match access.split() {
            Ok(access) => L::merge_ready(access, queue),
            Err(access) => R::merge_ready(access, queue),
        }
    }

    fn restore<'queue, S>(access: selected::Access<'_, Self>, dequeued: S::Dequeued<'queue, Global>)
    where
        S: queue::Backend<'d> + 'queue,
    {
        match access.split() {
            Ok(access) => L::restore::<S>(access, dequeued),
            Err(access) => R::restore::<S>(access, dequeued),
        }
    }
}
