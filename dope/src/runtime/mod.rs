pub mod dispatcher;
pub mod executor;
pub mod launcher;
pub mod profile;
mod run;
mod signal;
pub mod trigger;

#[doc(hidden)]
pub mod __private {
    use std::pin::Pin;
    use std::time::{Duration, Instant};

    use crate::DriverContext;
    use crate::driver::token::Token;

    const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

    pub trait RootTask<'d, T> {
        fn target(&self) -> Token;
        fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);
        fn take_output(self: Pin<&mut Self>) -> Option<T>;
    }

    pub struct Deadline;

    impl Deadline {
        pub fn after(base: Instant, duration: Duration) -> Instant {
            base.checked_add(duration)
                .or_else(|| base.checked_add(FAR_FUTURE))
                .unwrap_or(base)
        }
    }
}
