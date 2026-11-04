use std::ptr;

pub trait Contract {
    fn identity(&self) -> Option<ptr::NonNull<()>> {
        None
    }

    fn is_runnable(&self, _: impl FnOnce() -> bool) -> bool
    where
        Self: Sized,
    {
        false
    }
}
