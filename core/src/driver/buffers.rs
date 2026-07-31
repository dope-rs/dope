use crate::backend::Backend;
use crate::backend::ops::buffers::BufferBackend;

use super::DriverContext;

pub trait ProvidedBuffers {
    fn buffer_group(&self) -> u16;
    fn buffer_len(&self) -> usize;
    fn buffer_count(&self) -> usize;
}

impl ProvidedBuffers for DriverContext<'_, '_> {
    fn buffer_group(&self) -> u16 {
        <Backend as BufferBackend>::buffer_group(self.backend_ref())
    }

    fn buffer_len(&self) -> usize {
        <Backend as BufferBackend>::buffer_len(self.backend_ref())
    }

    fn buffer_count(&self) -> usize {
        <Backend as BufferBackend>::buffer_count(self.backend_ref())
    }
}
