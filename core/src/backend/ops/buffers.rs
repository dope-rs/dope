use crate::backend::{Backend, RecvBuffer};

pub(crate) trait BufferBackend {
    fn buffer_group(backend: &Backend) -> u16;
    fn buffer_len(backend: &Backend) -> usize;
    fn buffer_count(backend: &Backend) -> usize;
    fn release_buffer(backend: &mut Backend, buffer: RecvBuffer);
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Backend, BufferBackend, RecvBuffer};
    use crate::backend::uring::provided::ffi::ring::RegisteredRing;

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            RegisteredRing::BGID
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.ring.provided().buf_len()
        }

        fn buffer_count(backend: &Backend) -> usize {
            backend.ring.provided().entries()
        }

        fn release_buffer(backend: &mut Backend, buffer: RecvBuffer) {
            backend.ring.provided_mut().defer(buffer);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use super::{Backend, BufferBackend, RecvBuffer};
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;

    impl BufferBackend for Backend {
        fn buffer_group(_backend: &Backend) -> u16 {
            0
        }

        fn buffer_len(backend: &Backend) -> usize {
            backend.recv.buf_len()
        }

        fn buffer_count(backend: &Backend) -> usize {
            backend.recv.entries()
        }

        fn release_buffer(backend: &mut Backend, buffer: RecvBuffer) {
            backend.recv.defer(buffer);
            if !backend.resume.is_empty() {
                backend.resume_pending();
            }
        }
    }
}
