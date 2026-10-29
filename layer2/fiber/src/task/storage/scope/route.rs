use std::{ops, pin};

use o3::base::cell::region;

use crate::{
    context::wake,
    core::driver::schedule::ready::task,
    task::storage::{
        fixed::{
            direct, local, quota,
            schedule::{self, queue},
            tree,
        },
        scope::{Domain, Scope, Scoped},
    },
};

/// Access to one routed member while its plane retains the identity brand.
pub struct Route<'s, R> {
    inner: Scope<'s, R>,
}

impl<'s, R> Route<'s, R> {
    pub(super) fn new(source: R, proof: region::Proof<region::Identity<'s>>) -> Self {
        Self {
            inner: Scope::new(source, proof),
        }
    }
}

impl<'s, R> ops::Deref for Route<'s, R> {
    type Target = Scope<'s, R>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<R> ops::DerefMut for Route<'_, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'s, 'a, 'd, R, P, Q> Route<'s, direct::Route<'a, 'd, R, P, Q>>
where
    R: tree::At<'d, P>,
    Scope<'s, direct::Route<'a, 'd, R, P, Q>>: direct::Source<
            'd,
            Domain<'s, <R::Member as tree::Member<'d>>::Domain>,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        >,
{
    pub fn admit(
        &mut self,
    ) -> Option<
        impl direct::Insert<
            'd,
            Domain<'s, <R::Member as tree::Member<'d>>::Domain>,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        >,
    > {
        direct::Source::admit(pin::Pin::new(&mut self.inner))
    }

    pub fn insert(
        &mut self,
        fiber: <R::Member as tree::Member<'d>>::Fiber,
    ) -> Option<Scoped<'s, 'd, <R::Member as tree::Member<'d>>::Domain>> {
        local::Store::insert(pin::Pin::new(&mut self.inner), fiber)
    }

    pub fn remove(
        &mut self,
        id: Scoped<'s, 'd, <R::Member as tree::Member<'d>>::Domain>,
    ) -> local::Removal {
        local::Store::remove(pin::Pin::new(&mut self.inner), id)
    }
}

impl<'s, 'a, 'd, R, P, Q, S, M> Route<'s, schedule::Route<'a, 'd, R, P, Q, S>>
where
    R: tree::At<'d, P, Member = M>,
    M: schedule::Ops<'d> + 'a,
    Q: quota::State,
    S: queue::Backend<'d>,
    'd: 'a,
{
    pub fn try_admit(
        &mut self,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(schedule::Input<'d, M>) -> Scoped<'s, 'd, M::Domain>, S::Error> {
        let proof = self.inner.proof;
        let permit = self.inner.source.try_admit(input, parent)?;
        Ok(move |input| permit(input).bind(proof))
    }

    pub fn remove(&mut self, id: Scoped<'s, 'd, M::Domain>) -> local::Removal {
        self.inner.source.remove(id.original())
    }
}

impl<'s, 'a, 'd, R, P: 'a> Route<'s, schedule::Route<'a, 'd, R, P>>
where
    R: tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    'd: 'a,
{
    pub fn try_spawn(
        self,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(schedule::Input<'d, R::Member>) + 'a, task::Error> {
        self.inner.source.try_spawn(parent)
    }
}
