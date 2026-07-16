mod dispatcher;
mod executor;
mod ffi;
mod launcher;
pub mod profile;
mod trigger;

pub use dispatcher::{Dispatcher, Idle};
pub use executor::{AppSession, Executor, Session, StorageFactory, ValueStorage};
pub use launcher::{Launcher, WorkerContext, WorkerEntry};
pub use trigger::{ShutdownTrigger, SignalShutdown};

#[doc(hidden)]
pub mod __private {
    use std::pin::Pin;
    use std::time::{Duration, Instant};

    use crate::DriverContext;
    use crate::driver::token;

    const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

    pub trait RootTask<'d, T> {
        fn target(&self) -> token::Token;
        fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);
        fn take_output(self: Pin<&mut Self>) -> Option<T>;
    }

    pub fn saturating_deadline(base: Instant, duration: Duration) -> Instant {
        base.checked_add(duration)
            .or_else(|| base.checked_add(FAR_FUTURE))
            .unwrap_or(base)
    }
}
