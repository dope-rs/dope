use std::{pin, task};

use crate::{
    abi, context,
    task::storage::{
        Id,
        fixed::{self, Capacity, local},
        scope::{Domain, Scope, Scoped},
    },
};

/// Terminal state of a task identity consumed by its owning store.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Removal {
    Removed,
    Absent,
}

/// Task operations whose identities retain their original storage scope.
pub trait Store<'d, D> {
    type Fiber;

    fn capacity(&self) -> Capacity;
    fn insert(self: pin::Pin<&mut Self>, fiber: Self::Fiber) -> Option<Id<'d, D>>;
    /// Removes by identity, reporting absence instead of returning the identity.
    fn remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Removal {
        match self.try_remove(id) {
            Ok(()) => Removal::Removed,
            Err(_) => Removal::Absent,
        }
    }
    fn try_remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Result<(), Id<'d, D>>;
}

mod core;
mod next;
pub(in crate::task::storage) use next::Next;

pub(in crate::task::storage) use crate::task::storage::fixed::local::core::Core;

pub trait Stream<'d, D>: Store<'d, D, Fiber: abi::Stream<'d>> {
    fn poll_next(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<Option<<Self::Fiber as abi::Stream<'d>>::Item>>>;
}

impl<'s, 'd, D, F, S: local::Core<'d, D, Fiber = F> + Unpin> local::Store<'d, Domain<'s, D>>
    for Scope<'s, S>
{
    type Fiber = F;

    fn capacity(&self) -> fixed::Capacity {
        self.source.capacity()
    }

    fn insert(self: pin::Pin<&mut Self>, fiber: Self::Fiber) -> Option<Scoped<'s, 'd, D>> {
        let this = self.get_mut();
        pin::Pin::new(&mut this.source)
            .insert(fiber)
            .map(|id| id.bind(this.proof))
    }

    fn try_remove(
        self: pin::Pin<&mut Self>,
        id: Scoped<'s, 'd, D>,
    ) -> Result<(), Scoped<'s, 'd, D>> {
        let this = self.get_mut();
        pin::Pin::new(&mut this.source)
            .try_remove(id.original())
            .map_err(|id| id.bind(this.proof))
    }
}

impl<'s, 'd, D, S: local::Next<'d, D> + Unpin> Stream<'d, Domain<'s, D>> for Scope<'s, S> {
    fn poll_next(
        self: pin::Pin<&mut Self>,
        id: &Scoped<'s, 'd, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<Option<<Self::Fiber as abi::Stream<'d>>::Item>>> {
        pin::Pin::new(&mut self.get_mut().source).poll_next(&id.original(), context)
    }
}
