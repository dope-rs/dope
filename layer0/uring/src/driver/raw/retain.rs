use dope_sys::{
    engine::admit::{self, Context},
    io::{self, reactor::submit, socket::addr},
    table::token::Codec,
};

use o3::buffer::bytes;

use crate::{Uring, driver::Key, reactor::sq};

impl<C, T> admit::raw::Retain<C> for sq::raw::Vacancy<'_, T>
where
    C: Context<Native = Uring, Key = Key<T>>,
    T: Codec,
{
    unsafe fn send(
        self,
        slot: sq::Fixed,
        input: bytes::Prefix<'_, { i32::MAX as usize }>,
        key: Key<T>,
    ) {
        // SAFETY: the host owns the input through the terminal flight.
        unsafe { self.push_retained(T::flight(key), |word| sq::Borrowed::send(slot, input, word)) };
    }

    unsafe fn send_msg<'a>(self, slot: sq::Fixed, message: impl io::raw::Message<'a>, key: Key<T>) {
        // SAFETY: the host retains the complete immutable message graph.
        unsafe {
            self.push_retained(T::flight(key), |word| {
                sq::Borrowed::send_msg(slot, message, word)
            })
        };
    }

    unsafe fn accept(self, listener: sq::Fixed, peer: &mut addr::Storage, key: Key<T>) {
        // SAFETY: the host retains exclusive access to this peer output.
        unsafe {
            self.push_retained(T::flight(key), |word| {
                sq::Borrowed::accept(listener, peer, word)
            })
        };
    }

    unsafe fn connect(self, slot: sq::Fixed, address: addr::Addr<'_>, key: Key<T>) {
        // SAFETY: the host retains the address through this flight.
        let request = unsafe { sq::raw::Deferred::new(slot, address) };
        request.submit(self, T::flight(key));
    }

    unsafe fn file(self, request: sq::Borrowed<'_>, key: Key<T>) {
        // SAFETY: the host retains every encoded file borrow through this flight's completion.
        unsafe { submit::raw::Vacancy::push(self, request, T::flight(key)) };
    }
}
