use std::ptr;

pub trait Contract {
    fn identity(&self) -> Option<ptr::NonNull<()>> {
        None
    }
}
