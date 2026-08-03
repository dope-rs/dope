use core::task::Poll;

use o3::cell::{RegionCell, RegionToken};

use crate::abi::pollfn::PollFn;

/// Scoped access to executor-local state.
///
/// Values borrowed through this context cannot escape the callback. This is
/// the application-facing replacement for storing and dereferencing raw
/// pointers to runtime state.
pub struct LocalContext<'a, 'd> {
    region: &'a mut RegionToken<'d>,
}

impl<'a, 'd> LocalContext<'a, 'd> {
    #[doc(hidden)]
    pub fn from_region(region: &'a mut RegionToken<'d>) -> Self {
        Self { region }
    }

    pub fn reborrow(&mut self) -> LocalContext<'_, 'd> {
        LocalContext {
            region: self.region,
        }
    }
}

/// Executor-region-owned mutable state with closure-scoped safe access.
#[repr(transparent)]
pub struct LocalCell<'d, T> {
    inner: RegionCell<'d, T>,
}

impl<'d, T> LocalCell<'d, T> {
    pub const fn new(value: T) -> Self {
        Self {
            inner: RegionCell::new(value),
        }
    }

    pub fn read_with<R>(
        &self,
        context: &LocalContext<'_, 'd>,
        f: impl for<'a> FnOnce(&'a T) -> R,
    ) -> R {
        f(self.inner.borrow(context.region))
    }

    pub fn write_with<R>(
        &self,
        context: &mut LocalContext<'_, 'd>,
        f: impl for<'a> FnOnce(&'a mut T) -> R,
    ) -> R
    where
        T: Unpin,
    {
        f(self.inner.borrow_mut(context.region))
    }

    /// Runs one immutable access when the returned fiber is polled.
    pub fn read<'a, F, R>(
        &'a self,
        f: F,
    ) -> PollFn<'d, impl FnMut(core::pin::Pin<&mut crate::Context<'_, 'd>>) -> Poll<R> + 'a, R>
    where
        F: for<'r> FnOnce(&'r T) -> R + 'a,
        T: 'a,
    {
        let mut f = Some(f);
        PollFn::new(move |mut cx| {
            let f = f.take().expect("local read fiber polled after completion");
            let context = LocalContext::from_region(cx.as_mut().region_token());
            Poll::Ready(self.read_with(&context, f))
        })
    }

    /// Runs one mutable access when the returned fiber is polled.
    pub fn write<'a, F, R>(
        &'a self,
        f: F,
    ) -> PollFn<'d, impl FnMut(core::pin::Pin<&mut crate::Context<'_, 'd>>) -> Poll<R> + 'a, R>
    where
        F: for<'r> FnOnce(&'r mut T) -> R + 'a,
        T: Unpin + 'a,
    {
        let mut f = Some(f);
        PollFn::new(move |mut cx| {
            let f = f.take().expect("local write fiber polled after completion");
            let mut context = LocalContext::from_region(cx.as_mut().region_token());
            Poll::Ready(self.write_with(&mut context, f))
        })
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}
