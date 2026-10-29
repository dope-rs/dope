use std::marker;

use dope_sys::{memory, memory::buffer};
use o3::buffer::{pool, pool::state};

use crate::store::buffer::Buffer;

mod raw;

#[repr(transparent)]
pub struct Pool<L> {
    inner: pool::Pool<state::Initialized>,
    layout: marker::PhantomData<fn() -> L>,
}

impl<L: memory::raw::Layout> buffer::Pool for Pool<L> {
    type Layout = L;
    type Buffer = Buffer;
    type Context = raw::Context<L>;

    fn try_new(layout: L) -> Result<(Self, Self::Context), pool::CreateError> {
        let entries = usize::from(layout.entries().get());
        let capacity = layout.capacity().get() as usize;
        let inner = pool::Pool::try_new(entries, capacity)?;
        Ok((
            Self {
                inner,
                layout: marker::PhantomData,
            },
            raw::Context(layout),
        ))
    }

    fn take(&self) -> Option<Buffer> {
        self.inner.acquire().map(Buffer)
    }
}
