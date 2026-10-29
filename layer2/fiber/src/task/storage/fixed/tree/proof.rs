use crate::task::storage::fixed::tree::{Branch, Here, Leaf, Left, Right};

pub trait Proof<P = ()> {}

impl<S> Proof for Leaf<S> {}

impl<L, R> Proof for Branch<L, R> {}

impl<S> Proof<Here> for Leaf<S> {}

impl<L: Proof<P>, R, P> Proof<Left<P>> for Branch<L, R> {}

impl<L, R: Proof<P>, P> Proof<Right<P>> for Branch<L, R> {}
