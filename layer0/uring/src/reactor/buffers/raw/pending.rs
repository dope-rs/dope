use std::{marker, num};

use dope_sys::{memory, memory::buffer};
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::reactor::buffers::Selected;

impl<E, C> buffer::raw::Pending for Selected<E, C>
where
    E: memory::raw::Extent<Capacity = C>,
    C: Copy + Into<num::NonZero<u32>>,
{
    type Extent = E;
    type Complete = Selected<E, <E as memory::raw::Extent>::Length>;

    fn try_complete(
        self,
        len: <E as memory::raw::Extent>::Length,
    ) -> Result<
        Self::Complete,
        (
            Self,
            <E as memory::raw::Extent>::Length,
            <E as memory::raw::Extent>::Capacity,
        ),
    > {
        let capacity = self.len;
        let count: bounded::U32<0, { MAX as u32 }> = len.into();
        let maximum: num::NonZero<u32> = capacity.into();
        if count.get() > maximum.get() {
            return Err((self, len, capacity));
        }
        Ok(Selected {
            index: self.index,
            len,
            extent: marker::PhantomData,
        })
    }
}
