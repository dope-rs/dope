use crate::backend::Backend;

pub(crate) trait BufferBackend {
    fn buffer_group(backend: &Backend) -> u16;
    fn buffer_len(backend: &Backend) -> usize;
    unsafe fn release_buffer(backend: &mut Backend, bid: u16);
}

#[cfg(target_os = "linux")]
mod linux {
    use crate::backend::uring::provided::ffi::ring::ProvidedRing;

    use super::{Backend, BufferBackend};

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            ProvidedRing::BGID
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.provided.buf_len()
        }

        unsafe fn release_buffer(backend: &mut Backend, bid: u16) {
            backend.provided.defer(bid);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;

    use super::{Backend, BufferBackend};

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            0
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.provided.buf_len()
        }

        unsafe fn release_buffer(backend: &mut Backend, bid: u16) {
            backend.provided.defer(bid);
            if !backend.resume.is_empty() {
                backend.resume_pending();
            }
        }
    }
}
