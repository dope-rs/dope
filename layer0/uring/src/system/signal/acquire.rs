use std::mem;

use dope_sys::Errno;

#[derive(Debug)]
pub enum Acquire {
    MaskBlock(Errno),
    SignalFd(Errno),
}

const _: () = {
    assert!(mem::size_of::<Acquire>() == 8);
    assert!(mem::align_of::<Acquire>() == mem::align_of::<Errno>());
    assert!(mem::size_of::<Result<(), Acquire>>() == 8);
};

use dope_sys::thread::signal::AcquireFault;

impl From<Acquire> for AcquireFault {
    fn from(error: Acquire) -> Self {
        match error {
            Acquire::MaskBlock(error) => Self::MaskBlock(error),
            Acquire::SignalFd(error) => Self::SignalFd(error),
        }
    }
}
