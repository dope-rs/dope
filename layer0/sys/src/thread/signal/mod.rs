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

/// Exact failure to restore the policy retained by a signal receiver.
#[derive(Debug)]
pub enum ReleaseFault {
    Mask(crate::Errno),
    Disposition(crate::Errno),
}
