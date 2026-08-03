use std::io;

use dope_core::driver::control::ContextControl;
use dope_core::io::pipe::Pipe;
use o3::marker::ThreadBound;

use super::signal::SignalState;
use crate::DriverContext;

/// A cloneable, process-local shutdown notification.
///
/// Every runtime worker registers a clone with its driver. Firing any clone
/// makes every registered worker observe the same shutdown request.
pub struct ShutdownTrigger {
    pipe: Pipe,
}

impl ShutdownTrigger {
    pub fn new() -> io::Result<Self> {
        Ok(Self { pipe: Pipe::new()? })
    }

    pub fn try_register(&self, driver: &mut DriverContext<'_, '_>) -> io::Result<()> {
        driver.register_shutdown_fd(self.pipe.read_end())
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            pipe: self.pipe.try_clone()?,
        })
    }

    pub fn fire(&self) -> io::Result<()> {
        self.pipe.notify()
    }
}

/// A thread-bound shutdown source for `SIGINT` and `SIGTERM`.
///
/// Construction blocks both signals on the current thread. Dropping the value
/// closes the signal fd and restores the exact signal mask that was present at
/// construction time. Only one source may be live on a thread at once.
pub struct SignalShutdown {
    state: SignalState,
    _thread: ThreadBound,
}

impl SignalShutdown {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            state: SignalState::new()?,
            _thread: ThreadBound::NEW,
        })
    }

    pub fn try_register(&self, driver: &mut DriverContext<'_, '_>) -> io::Result<()> {
        self.state.try_register(driver)
    }
}
