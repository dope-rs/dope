use std::mem;

use dope_sys::{
    memory,
    memory::{
        buffer,
        buffer::{
            canary,
            provided::{Context as _, Open as _, Producer as _},
        },
    },
};

use crate::reactor::{
    buffers::{self, Canary, Create, Producer},
    cq::Buffer,
    register,
};

impl<L: memory::raw::Canary> canary::raw::Storage for Canary<L> {
    type Layout = L;
    type Index = Buffer;
    type Error = Create<L>;
    type Buffer<const GROUP: u16> = register::raw::Buffer<Self, GROUP>;

    fn new() -> Result<Self, Create<L>> {
        let context = buffers::raw::Context::new(L::LAYOUT)?;
        let producer = match Producer::new(&context) {
            Ok(producer) => producer,
            Err(state) => return Err(state.close(context)),
        };
        Ok(Self { producer, context })
    }

    fn into_buffer<const GROUP: u16>(self) -> Self::Buffer<GROUP> {
        let (address, entries) = self.producer.registration();
        // SAFETY: this owner retains the stable entry and payload mappings
        // together until the registration is removed or its ring is closed.
        unsafe { register::raw::Buffer::new(self, address, entries) }
    }

    unsafe fn option(&mut self) -> &libc::c_int {
        const { assert!(mem::align_of::<libc::c_int>() <= 4096) };
        let value = self.context.as_ptr().cast_mut().cast::<libc::c_int>();
        // SAFETY: Canary's layout admits this scalar in its page-aligned mapping;
        // the caller excludes every outstanding kernel access and scalar borrow.
        unsafe {
            value.write(1);
            &*value
        }
    }

    unsafe fn inspect<E>(
        &mut self,
        index: Buffer,
        len: <L::Extent as memory::raw::Extent>::Length,
        check: impl FnOnce(&[u8]) -> Result<(), E>,
    ) -> Result<
        (),
        canary::Failure<
            Buffer,
            <L::Extent as memory::raw::Extent>::Length,
            <L::Extent as memory::raw::Extent>::Capacity,
            E,
        >,
    > {
        use buffer::raw::{Context as _, Pending as _};
        // SAFETY: the caller's unique CQE transfers any valid selected slot.
        let selected = unsafe { buffer::raw::Select::select(&self.producer, index) }
            .map_err(canary::Failure::Index)?;
        let selected = match selected.try_complete(len) {
            Ok(selected) => selected,
            Err((selected, actual, capacity)) => {
                // SAFETY: the rejected length retains this producer's unique slot.
                unsafe { buffer::raw::Release::release(&mut self.producer, selected) };
                self.producer.flush();
                return Err(canary::Failure::Length { actual, capacity });
            }
        };
        // SAFETY: the selection above came from this storage's original producer;
        // its paired context remains live and completion admitted this prefix.
        let result = check(unsafe { self.context.bytes(&selected) });
        // SAFETY: inspection ended every projection of this producer's unique slot.
        unsafe { buffer::raw::Release::release(&mut self.producer, selected) };
        self.producer.flush();
        result.map_err(canary::Failure::Inspect)
    }
}
