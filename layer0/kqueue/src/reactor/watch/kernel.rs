use dope_sys::{Errno, io::reactor::ready::delivery, kernel};

#[repr(transparent)]
pub struct Kernel<'a>(pub(in crate::reactor) &'a libc::kevent);

impl delivery::Event for Kernel<'_> {
    fn filter(&self) -> i16 {
        self.0.filter
    }

    fn error(&self) -> Option<Errno> {
        if self.0.flags & libc::EV_ERROR == 0 {
            return None;
        }
        // SAFETY: the kernel reports EV_ERROR data as zero or one positive errno.
        unsafe { kernel::raw::Code::from_native(self.0.data) }.map(Into::into)
    }

    fn data(self) -> usize {
        self.0.udata.addr()
    }
}
