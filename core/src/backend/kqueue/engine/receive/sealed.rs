use std::{io, ptr};

use o3::buffer::{
    self,
    pool::{self, state},
};

use crate::{driver::settings, io::recv};

pub(in crate::backend::kqueue) struct Pool {
    inner: buffer::Pool<state::Initialized>,
}

impl Pool {
    pub(in crate::backend::kqueue) fn try_new(receive: settings::Receive) -> io::Result<Self> {
        let entries = usize::from(receive.entries());
        let capacity = receive.nonzero_buffer_len().get() as usize;
        let layout = buffer::Layout::new(entries, capacity).map_err(pool::CreateError::Layout)?;
        Ok(Self {
            inner: buffer::Pool::try_from_layout(layout).map_err(pool::CreateError::Allocation)?,
        })
    }

    pub(in crate::backend::kqueue::engine) fn take(
        &self,
    ) -> Option<buffer::Lease<state::Initialized>> {
        self.inner.try_acquire()
    }

    pub(in crate::backend::kqueue) fn region(
        &self,
        token: &mut buffer::Lease<state::Initialized>,
        len: usize,
    ) -> recv::raw::Region {
        let len = len.min(self.inner.capacity()).min(token.capacity());
        let bytes = &mut token.spare_mut()[..len];
        let len = bytes.len();
        let pointer = ptr::NonNull::from(bytes).cast();
        // SAFETY: the lease retained by the receive permit owns this region.
        unsafe { recv::raw::Region::new(pointer, len) }
    }
}
