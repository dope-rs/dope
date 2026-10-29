use std::pin;

use ready::{Global, Path};
use storage::fixed::{local::Removal, tree};

use crate::{
    abi,
    context::wake,
    core::driver::{
        retained,
        schedule::{
            app::{self, step},
            ready::task,
        },
    },
    task::storage::{self, Id},
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
pub use pool::Pool;
pub use route::Route;

pub(in crate::task::storage) type Input<'d, M> = (
    <M as tree::Member<'d>>::Fiber,
    <M as tree::Member<'d>>::Target,
);

type Identity<'d, M> = Id<'d, <M as tree::Member<'d>>::Domain>;

/// Operations supplied by one fixed scheduler leaf.
pub trait Ops<'d>: tree::Member<'d, Fiber: abi::Fiber<'d>> + pool::Proof + Sized {
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
        R: tree::At<'d, P, Member = Self>;

    fn drive_index<'turn, 'queue, S>(
        entry: selected::Entry<'_, Self>,
        dequeued: S::Dequeued<'queue, Global>,
        work: app::Work<'turn, 'd>,
        permit: step::Delegated<'_, 'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> result::Outcome<Self::Target, <Self::Fiber as abi::Fiber<'d>>::Output>
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

/// Recursive operations over a fixed scheduler tree.
pub trait PlaneTree<'d>: tree::Build<'d> {
    fn wake_global(access: selected::Access<'_, Self>);

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
    fn wake_global(access: selected::Access<'_, Self>) {
        match access.split() {
            Ok(access) => L::wake_global(access),
            Err(access) => R::wake_global(access),
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
