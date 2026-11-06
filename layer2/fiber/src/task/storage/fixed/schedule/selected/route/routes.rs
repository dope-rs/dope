use std::marker;

use schedule::pool::Proof as _;
use selected::{Claim, State, route};

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
        state: State<'plane, 'turn, 'd, R, Q, S, TreeIndex<T>>,
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
        state: State<'plane, 'turn, 'd, R, Q, S, TreeIndex<tree::Leaf<M>>>,
    ) -> Self::Output {
        let State {
            active,
            dequeued,
            work,
            permit,
        } = state;
        let dequeued = S::map(dequeued, |index| route::Index {
            resident: index.resident(),
            root: marker::PhantomData,
            path: marker::PhantomData,
        });
        self.into_inner()(
            context,
            route::Entry {
                claim: Claim::new(active, dequeued, work, permit),
            },
        )
    }
}

impl<'plane, 'turn, 'd, R, L, T, Q, S, C, X, A, B, O>
    Dispatch<'plane, 'turn, 'd, R, tree::Branch<L, T>, Q, S, C, X> for tree::Branch<A, B>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, X::Path<tree::Right<T::First>>>,
    R::Member: schedule::pool::Proof,
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
        state: State<'plane, 'turn, 'd, R, Q, S, TreeIndex<tree::Branch<L, T>>>,
    ) -> Self::Output {
        let (left, right) = self.into_parts();
        let State {
            active,
            dequeued,
            work,
            permit,
        } = state;
        let right_base = R::project_ref(&active.as_ref().get_ref().routes).base();
        match S::branch(dequeued, |index| index.split(right_base)) {
            Ok(dequeued) => left.dispatch(
                context,
                State {
                    active,
                    dequeued,
                    work,
                    permit,
                },
            ),
            Err(dequeued) => right.dispatch(
                context,
                State {
                    active,
                    dequeued,
                    work,
                    permit,
                },
            ),
        }
    }
}
