use crate::backend::Backend;
use crate::io::provided::raw::buffer::BufferId;

pub(crate) trait BufferBackend {
    fn buffer_group(backend: &Backend) -> u16;
    fn buffer_len(backend: &Backend) -> usize;
    fn release_buffer(backend: &mut Backend, id: BufferId);
}

#[cfg(target_os = "linux")]
mod linux {
    use crate::backend::uring::provided::ffi::ring::ProvidedRing;

    use super::{Backend, BufferBackend, BufferId};

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            ProvidedRing::BGID
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.provided.buf_len()
        }

        fn release_buffer(backend: &mut Backend, id: BufferId) {
            backend.provided.defer(id);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;

    use super::{Backend, BufferBackend, BufferId};

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            0
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.provided.buf_len()
        }

        fn release_buffer(backend: &mut Backend, id: BufferId) {
            backend.provided.defer(id);
            if !backend.resume.is_empty() {
                backend.resume_pending();
            }
        }
    }
}
