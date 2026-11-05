use std::{pin, task};

use crate::{
    context,
    task::storage::{
        Id,
        fixed::{local, staged},
    },
};

pub trait Core<'d, D>: local::Core<'d, D> {
    type Output;
    type Ready<'a>: staged::Completion<Output = Self::Output>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>>;

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> staged::Removal;
}

impl<'d, D, S: staged::Core<'d, D>> staged::Core<'d, D> for pin::Pin<&mut S> {
    type Output = S::Output;
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

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> staged::Removal {
        S::remove_state(self.get_mut().as_mut(), id)
    }
}
