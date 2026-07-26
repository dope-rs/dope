use crate::backend::Backend;
use crate::backend::ops::buffers::BufferBackend;

use super::DriverContext;

pub trait ProvidedBuffers {
    fn buffer_group(&self) -> u16;
    fn buffer_len(&self) -> usize;
    /// # Safety
    /// `bid` must name a live buffer uniquely owned by the caller.
    unsafe fn release(&mut self, bid: u16);
}

impl ProvidedBuffers for DriverContext<'_, '_> {
    fn buffer_group(&self) -> u16 {
        <Backend as BufferBackend>::buffer_group(self.backend_ref())
    }

    fn buffer_len(&self) -> usize {
        <Backend as BufferBackend>::buffer_len(self.backend_ref())
    }

    unsafe fn release(&mut self, bid: u16) {
        unsafe { <Backend as BufferBackend>::release_buffer(self.backend(), bid) };
    }
}
