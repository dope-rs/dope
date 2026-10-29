use dope_sys::{
    engine::admit::{self, Context},
    io::reactor::submit,
    table::token::Codec,
};

use crate::{Uring, driver::Key, reactor::sq};

impl<C, T> admit::raw::Retain<C> for sq::raw::Vacancy<'_, T>
where
    C: Context<Native = Uring, Key = Key<T>>,
    T: Codec,
{
    unsafe fn retain(self, request: sq::Borrowed<'_>, key: Key<T>) {
        // SAFETY: the host retains every encoded pointee through this flight's completion.
        unsafe { submit::raw::Vacancy::push(self, request, T::flight(key)) };
    }

    unsafe fn file(self, request: sq::Borrowed<'_>, key: Key<T>) {
        // SAFETY: the host retains every encoded file borrow through this flight's completion.
        unsafe { submit::raw::Vacancy::push(self, request, T::flight(key)) };
    }
}
