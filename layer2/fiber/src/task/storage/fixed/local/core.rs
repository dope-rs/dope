use std::pin;

use crate::task::storage::{
    Id,
    fixed::{self, Capacity, local},
};

pub trait Core<'d, D> {
    type Fiber;

    fn capacity(&self) -> Capacity;
    fn insert(self: pin::Pin<&mut Self>, fiber: Self::Fiber) -> Option<Id<'d, D>>;
    fn try_remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Result<(), Id<'d, D>>;
}

impl<'d, D, S: local::Core<'d, D>> local::Core<'d, D> for pin::Pin<&mut S> {
    type Fiber = S::Fiber;

    fn capacity(&self) -> fixed::Capacity {
        S::capacity(self.as_ref().get_ref())
    }

    fn insert(self: pin::Pin<&mut Self>, fiber: Self::Fiber) -> Option<Id<'d, D>> {
        S::insert(self.get_mut().as_mut(), fiber)
    }

    fn try_remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Result<(), Id<'d, D>> {
        S::try_remove(self.get_mut().as_mut(), id)
    }
}
