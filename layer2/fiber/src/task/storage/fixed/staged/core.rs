use std::{pin, task};

use crate::{
    abi, context,
    task::storage::{
        Id,
        fixed::{local, staged},
    },
};

pub trait Core<'d, D>: local::Core<'d, D, Fiber: abi::Fiber<'d>> {
    type Ready<'a>: staged::Completion<Output = <Self::Fiber as abi::Fiber<'d>>::Output>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>>;

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<staged::Poll>;
    fn take_output(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
    ) -> Option<<Self::Fiber as abi::Fiber<'d>>::Output>;
    fn state(&self, id: &Id<'d, D>) -> Option<staged::State>;

    /// Removes one identity, returning its phase before destruction.
    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Option<staged::State>;
}

impl<'d, D, S: staged::Core<'d, D>> staged::Core<'d, D> for pin::Pin<&mut S> {
    type Ready<'a>
        = S::Ready<'a>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>> {
        S::poll_ready(self.get_mut().as_mut(), id, context)
    }

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<staged::Poll> {
        S::poll_staged(self.get_mut().as_mut(), id, context)
    }

    fn take_output(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
    ) -> Option<<Self::Fiber as abi::Fiber<'d>>::Output> {
        S::take_output(self.get_mut().as_mut(), id)
    }

    fn state(&self, id: &Id<'d, D>) -> Option<staged::State> {
        S::state(self.as_ref().get_ref(), id)
    }

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Option<staged::State> {
        S::remove_state(self.get_mut().as_mut(), id)
    }
}
