use std::os::fd;

pub(crate) struct Descriptor<'a>(fd::BorrowedFd<'a>);

impl<'a> Descriptor<'a> {
    pub(super) const fn new(descriptor: fd::BorrowedFd<'a>) -> Self {
        Self(descriptor)
    }

    pub(super) fn read(&self, byte: &mut u8) -> isize {
        // SAFETY: `byte` names one writable byte and the borrowed descriptor
        // remains valid for the complete call.
        unsafe { libc::read(fd::AsRawFd::as_raw_fd(&self.0), (byte as *mut u8).cast(), 1) }
    }

    pub(super) fn write(&self, byte: &u8) -> isize {
        // SAFETY: `byte` names one readable byte and the borrowed descriptor
        // remains valid for the complete call.
        unsafe {
            libc::write(
                fd::AsRawFd::as_raw_fd(&self.0),
                (byte as *const u8).cast(),
                1,
            )
        }
    }
}
