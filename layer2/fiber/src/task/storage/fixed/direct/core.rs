use std::pin;

use crate::task::storage::fixed::{direct, local};

pub trait Core<'d, D>: local::Core<'d, D> {
    fn admit(self: pin::Pin<&mut Self>) -> Option<impl direct::Insert<'d, D, Fiber = Self::Fiber>>;
}

impl<'d, D, S: direct::Core<'d, D>> direct::Core<'d, D> for pin::Pin<&mut S> {
    fn admit(self: pin::Pin<&mut Self>) -> Option<impl direct::Insert<'d, D, Fiber = Self::Fiber>> {
        S::admit(self.get_mut().as_mut())
    }
}
