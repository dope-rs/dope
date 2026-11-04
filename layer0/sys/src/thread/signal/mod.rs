use std::os::fd;

pub mod raw;

pub trait Signal: fd::AsFd {
    type Error;

    /// Consumes a pending signal after descriptor readiness. The caller retains
    /// sole consumption authority between observing readiness and this call.
    fn wait(&self) -> Result<(), Self::Error>;
}

/// Complete acquisition-stage diagnostics shared by native signal receivers.
#[derive(Debug)]
pub enum AcquireFault {
    MaskBlock(crate::Errno),
    SignalFd(crate::Errno),
    QueueOpen(crate::Errno),
    QueueCloexec(crate::Errno),
    Register(crate::Errno),
    Ignore(crate::Errno),
}

const _: () = {
    assert!(std::mem::size_of::<AcquireFault>() == 8);
    assert!(std::mem::align_of::<AcquireFault>() == std::mem::align_of::<crate::Errno>());
    assert!(std::mem::size_of::<Result<(), AcquireFault>>() == 8);
};

/// Exact failure to restore the policy retained by a signal receiver.
#[derive(Debug)]
pub enum ReleaseFault {
    Mask(crate::Errno),
    Disposition(crate::Errno),
}
