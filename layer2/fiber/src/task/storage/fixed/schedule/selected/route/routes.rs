use std::marker;

use selected::{Claim, route};

use crate::task::storage::fixed::{
    quota,
    schedule::{self, queue, ready::TreeIndex, selected},
    tree,
};

pub(in crate::task::storage::fixed::schedule::selected) struct Root;

pub(in crate::task::storage::fixed::schedule::selected) struct Left<X>(
    marker::PhantomData<fn(X) -> X>,
);

pub(in crate::task::storage::fixed::schedule::selected) struct Right<X>(
    marker::PhantomData<fn(X) -> X>,
);

pub(in crate::task::storage::fixed::schedule::selected) trait Prefix {
    type Path<P>;
}

impl Prefix for Root {
    type Path<P> = P;
}

impl<X: Prefix> Prefix for Left<X> {
    type Path<P> = X::Path<tree::Left<P>>;
}

impl<X: Prefix> Prefix for Right<X> {
    type Path<P> = X::Path<tree::Right<P>>;
}

pub(in crate::task::storage::fixed::schedule::selected) struct Position<'d, R, T, X>
where
    R: schedule::PlaneTree<'d>,
    T: schedule::PlaneTree<'d>,
    X: Prefix,
{
    index: TreeIndex<T>,
    root: TreeIndex<R>,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    owner: marker::PhantomData<fn(R) -> R>,
    path: marker::PhantomData<fn(X) -> X>,
}

impl<'d, R, T, X> selected::Index<'d, R> for Position<'d, R, T, X>
where
    R: schedule::PlaneTree<'d>,
    T: schedule::PlaneTree<'d>,
    X: Prefix,
{
    fn into_root(self) -> TreeIndex<R> {
        self.root
    }
}

impl<'d, R: schedule::PlaneTree<'d>> From<TreeIndex<R>> for Position<'d, R, R, Root> {
    fn from(index: TreeIndex<R>) -> Self {
        Self {
            index,
            root: index,
            brand: marker::PhantomData,
            owner: marker::PhantomData,
            path: marker::PhantomData,
        }
    }
}

impl<'d, R, L, T, X> selected::Split<'d, R> for Position<'d, R, tree::Branch<L, T>, X>
where
    R: schedule::PlaneTree<'d>,
    L: schedule::PlaneTree<'d>,
    T: schedule::PlaneTree<'d>,
    X: Prefix,
{
    type Left = Position<'d, R, L, Left<X>>;
    type Right = Position<'d, R, T, Right<X>>;

    fn split(self) -> Result<Self::Left, Self::Right> {
        match self.index.split() {
            Ok(index) => Ok(Position {
                index,
                root: self.root,
                brand: marker::PhantomData,
                owner: marker::PhantomData,
                path: marker::PhantomData,
            }),
            Err(index) => Err(Position {
                index,
                root: self.root,
                brand: marker::PhantomData,
                owner: marker::PhantomData,
                path: marker::PhantomData,
            }),
        }
    }
}

impl<'d, R, M, X> From<Position<'d, R, tree::Leaf<M>, X>>
    for route::Index<'d, R, X::Path<tree::Here>>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, X::Path<tree::Here>, Member = M>,
    M: schedule::Ops<'d>,
    tree::Leaf<M>: schedule::PlaneTree<'d>,
    X: Prefix,
{
    fn from(position: Position<'d, R, tree::Leaf<M>, X>) -> Self {
        route::Index {
            resident: position.index.resident(),
            root: marker::PhantomData,
            path: marker::PhantomData,
        }
    }
}

pub(in crate::task::storage::fixed::schedule::selected) trait Dispatch<
    'plane,
    'turn,
    'd,
    R,
    T,
    Q,
    S,
    C,
    X,
> where
    R: schedule::PlaneTree<'d>,
    T: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    X: Prefix,
{
    type Output;

    fn dispatch(
        self,
        context: C,
        claim: Claim<'plane, 'turn, 'd, R, Q, S, Position<'d, R, T, X>>,
    ) -> Self::Output;
}

impl<'plane, 'turn, 'd, R, M, Q, S, C, X, F, O>
    Dispatch<'plane, 'turn, 'd, R, tree::Leaf<M>, Q, S, C, X> for tree::Leaf<F>
where
    'd: 'turn,
    R: schedule::PlaneTree<'d> + tree::At<'d, X::Path<tree::Here>, Member = M> + 'plane,
    M: schedule::Ops<'d>,
    tree::Leaf<M>: schedule::PlaneTree<'d>,
    Q: quota::State + 'plane,
    S: queue::Backend<'d> + 'plane,
    X: Prefix,
    F: FnOnce(C, route::Entry<'plane, 'turn, 'd, R, X::Path<tree::Here>, Q, S>) -> O,
{
    type Output = O;

    fn dispatch(
        self,
        context: C,
        claim: Claim<'plane, 'turn, 'd, R, Q, S, Position<'d, R, tree::Leaf<M>, X>>,
    ) -> Self::Output {
        self.into_inner()(context, route::Entry { claim: claim.map() })
    }
}

impl<'plane, 'turn, 'd, R, L, T, Q, S, C, X, A, B, O>
    Dispatch<'plane, 'turn, 'd, R, tree::Branch<L, T>, Q, S, C, X> for tree::Branch<A, B>
where
    R: schedule::PlaneTree<'d>,
    L: schedule::PlaneTree<'d>,
    T: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    X: Prefix,
    A: Dispatch<'plane, 'turn, 'd, R, L, Q, S, C, Left<X>, Output = O>,
    B: Dispatch<'plane, 'turn, 'd, R, T, Q, S, C, Right<X>, Output = O>,
{
    type Output = O;

    fn dispatch(
        self,
        context: C,
        claim: Claim<'plane, 'turn, 'd, R, Q, S, Position<'d, R, tree::Branch<L, T>, X>>,
    ) -> Self::Output {
        let (left, right) = self.into_parts();
        let selected::State {
            active,
            dequeued,
            work,
            permit,
        } = claim.take();
        match S::branch(dequeued, selected::Split::split) {
            Ok(dequeued) => left.dispatch(context, Claim::new(active, dequeued, work, permit)),
            Err(dequeued) => right.dispatch(context, Claim::new(active, dequeued, work, permit)),
        }
    }
}
