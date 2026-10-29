use std::{pin, task};

use crate::{
    abi, context,
    task::storage::{Id, fixed::local},
};

pub trait Next<'d, D>: local::Core<'d, D, Fiber: abi::Stream<'d>> {
    fn poll_next(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<Option<<Self::Fiber as abi::Stream<'d>>::Item>>>;
}

impl<'d, D, S: Next<'d, D>> Next<'d, D> for pin::Pin<&mut S> {
    fn poll_next(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<Option<<Self::Fiber as abi::Stream<'d>>::Item>>> {
        S::poll_next(self.get_mut().as_mut(), id, context)
    }
}
