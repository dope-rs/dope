use core::{pin, task};
use std::process;

use o3::cell::region;

use crate::{abi, context};

/// Scoped access to executor-local state that cannot escape its callback.
pub struct Context<'a, 'd> {
    region: &'a mut region::Token<'d>,
}

impl<'a, 'd> Context<'a, 'd> {
    #[doc(hidden)]
    pub fn from_region(region: &'a mut region::Token<'d>) -> Self {
        Self { region }
    }

    pub fn reborrow(&mut self) -> Context<'_, 'd> {
        Context {
            region: self.region,
        }
    }

    /// Runs one low-level driver-region operation without allowing the
    /// region borrow itself to escape this call.
    ///
    /// ```compile_fail
    /// use dope_fiber::task::local::Context;
    /// use o3::cell::region;
    ///
    /// fn escape<'a, 'd>(context: &'a mut Context<'_, 'd>) -> &'a mut region::Token<'d> {
    ///     context.with_region(|region| region)
    /// }
    /// ```
    #[doc(hidden)]
    pub fn with_region<R>(
        &mut self,
        operation: impl for<'r> FnOnce(&'r mut region::Token<'d>) -> R,
    ) -> R {
        operation(self.region)
    }
}

/// Executor-region-owned mutable state with closure-scoped safe access.
#[repr(transparent)]
pub struct Cell<'d, T> {
    inner: region::Value<'d, T>,
}

impl<'d, T> Cell<'d, T> {
    pub const fn new(value: T) -> Self {
        use o3::cell::region;
        Self {
            inner: region::Value::new(value),
        }
    }

    pub fn read_with<R>(&self, context: &Context<'_, 'd>, f: impl for<'a> FnOnce(&'a T) -> R) -> R {
        f(self.inner.borrow(context.region))
    }

    pub fn write_with<R>(
        &self,
        context: &mut Context<'_, 'd>,
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
    ) -> abi::PollFn<'d, impl FnMut(pin::Pin<&mut context::Context<'_, 'd>>) -> task::Poll<R> + 'a, R>
    where
        F: for<'r> FnOnce(&'r T) -> R + 'a,
        T: 'a,
    {
        let mut f = Some(f);
        abi::PollFn::new(move |mut cx| {
            let context = Context::from_region(cx.as_mut().region_token());
            let Some(f) = f.take() else {
                process::abort();
            };
            task::Poll::Ready(self.read_with(&context, f))
        })
    }

    /// Runs one mutable access when the returned fiber is polled.
    pub fn write<'a, F, R>(
        &'a self,
        f: F,
    ) -> abi::PollFn<'d, impl FnMut(pin::Pin<&mut context::Context<'_, 'd>>) -> task::Poll<R> + 'a, R>
    where
        F: for<'r> FnOnce(&'r mut T) -> R + 'a,
        T: Unpin + 'a,
    {
        let mut f = Some(f);
        abi::PollFn::new(move |mut cx| {
            let mut context = Context::from_region(cx.as_mut().region_token());
            let Some(f) = f.take() else {
                process::abort();
            };
            task::Poll::Ready(self.write_with(&mut context, f))
        })
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}
