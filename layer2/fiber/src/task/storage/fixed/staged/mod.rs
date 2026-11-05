use std::{pin, task};

use o3::depot::arena::slab::storage;

use crate::{
    abi, context,
    task::storage::{
        Id,
        fixed::{self, local, staged, store},
        scope::{Domain, Scope, Scoped},
        slots,
    },
};

pub use crate::task::storage::slots::Task;

/// The phase destroyed by a consuming task removal.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Removal {
    Absent,
    Live,
    Staged,
}

/// Consumes one retained output and its physical slot.
/// Dropping the capability leaves the output in its original slot.
pub trait Completion {
    type Output;

    fn finish(self) -> Self::Output;
    fn discard(self);
}

pub struct Ready<'a, F, O, D, S: storage::Storage> {
    member: store::Member<'a, Task<F, O>, D, S>,
}

impl<F, O, D, S: storage::Storage> Completion for Ready<'_, F, O, D, S> {
    type Output = O;

    fn finish(mut self) -> O {
        // SAFETY: poll_ready retained this exact completed slot's exclusive pin;
        // no operation can replace or poll its output before consumption.
        let output = unsafe { slots::raw::Done::exact(self.member.value()) }.take();
        self.member.discard();
        output
    }

    fn discard(self) {
        self.member.discard();
    }
}

/// In-place completion access retaining its original storage scope.
pub trait Store<'d, D>: local::Store<'d, D> {
    type Output;
    type Ready<'a>: Completion<Output = Self::Output>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>>;

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Removal;
}

mod core;
pub(in crate::task::storage) use crate::task::storage::fixed::staged::core::Core;

impl<'d, F, O, const N: usize, D, S: storage::Storage> staged::Core<'d, D>
    for fixed::Slab<'d, Task<F, O>, N, D, S>
where
    F: abi::Fiber<'d, Output = O>,
{
    type Output = O;
    type Ready<'a>
        = Ready<'a, F, O, D, S>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>> {
        let mut member = self.project().inner.member(id)?;
        let Some((_, fresh)) = member.value().poll(|fiber| {
            // SAFETY: Task admits only its live fiber and ends that polling
            // authority before retaining the completed output.
            unsafe { context::raw::Live::new(fiber).poll(context.as_mut()) }
        }) else {
            return Some(task::Poll::Pending);
        };
        Some(task::Poll::Ready((Ready { member }, fresh)))
    }

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Removal {
        let Some(member) = self.project().inner.member(&id) else {
            return Removal::Absent;
        };
        // An occupied fixed task is live or staged; taking its output consumes this anchor.
        let removed = if member.get().is_staged() {
            Removal::Staged
        } else {
            Removal::Live
        };
        member.discard();
        removed
    }
}

impl<'s, 'd, D, S: staged::Core<'d, D> + Unpin> staged::Store<'d, Domain<'s, D>> for Scope<'s, S> {
    type Output = S::Output;
    type Ready<'a>
        = S::Ready<'a>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Scoped<'s, 'd, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>> {
        pin::Pin::new(&mut self.get_mut().source).poll_ready(&id.original(), context)
    }

    fn remove_state(self: pin::Pin<&mut Self>, id: Scoped<'s, 'd, D>) -> Removal {
        pin::Pin::new(&mut self.get_mut().source).remove_state(id.original())
    }
}
