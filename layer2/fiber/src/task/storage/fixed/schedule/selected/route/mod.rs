mod routes;

use std::marker;

use routes::{Dispatch, Root};
use schedule::selected::{self, Claim, Item};

use crate::{
    core::driver::{retained, schedule::app::step},
    task::storage::{
        caps,
        fixed::{
            quota,
            schedule::{
                self, queue,
                ready::{Resident, TreeIndex},
            },
            tree,
        },
    },
};

pub struct Entry<'plane, 'turn, 'd, R, P, Q = quota::Physical, S = queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    pub(super) claim: Claim<'plane, 'turn, 'd, R, Q, S, Index<'d, R, P>>,
}

pub(super) struct Index<'d, R: tree::At<'d, P>, P> {
    pub(super) resident: Resident<R::Member>,
    root: marker::PhantomData<fn(R) -> R>,
    path: marker::PhantomData<fn(P) -> P>,
}

impl<'d, R, P> selected::Index<'d, R> for Index<'d, R, P>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
{
    fn into_root(self) -> TreeIndex<R> {
        self.resident.root(caps::Path::<R, P>::new())
    }
}

impl<'d, M: tree::Member<'d>> From<TreeIndex<tree::Leaf<M>>>
    for Index<'d, tree::Leaf<M>, tree::Here>
{
    fn from(index: TreeIndex<tree::Leaf<M>>) -> Self {
        Self {
            resident: index.resident(),
            root: marker::PhantomData,
            path: marker::PhantomData,
        }
    }
}

pub trait Routes<'plane, 'turn, 'd, R, Q, S, C>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    type Output;

    fn run(self, context: C, selected: Item<'plane, 'turn, 'd, R, Q, S>) -> Self::Output;
}

impl<'plane, 'turn, 'd, R, Q, S, C, V, O> Routes<'plane, 'turn, 'd, R, Q, S, C> for V
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    V: Dispatch<'plane, 'turn, 'd, R, R, Q, S, C, Root, Output = O>,
{
    type Output = O;

    fn run(self, context: C, selected: Item<'plane, 'turn, 'd, R, Q, S>) -> Self::Output {
        Dispatch::dispatch(self, context, selected.claim.map())
    }
}

impl<'plane, 'turn, 'd, R, P, Q, S> Entry<'plane, 'turn, 'd, R, P, Q, S>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    pub fn target(&self) -> <R::Member as tree::Member<'d>>::Target {
        schedule::Ops::target_index(self.claim.entry_ref())
    }

    pub fn step(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> (selected::Completed<'d, R::Member>, step::Receipt<'turn, 'd>) {
        self.claim.step(driver)
    }
}

impl<'plane, 'turn, 'd, R, P, Q> Entry<'plane, 'turn, 'd, R, P, Q, queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    Q: quota::State,
{
    pub fn drive(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> selected::Completed<'d, R::Member> {
        self.claim.drive(driver)
    }
}
