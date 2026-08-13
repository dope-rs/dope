use std::{marker, pin, ptr};

use o3::cell::{brand, region};

use crate::driver::{self, lifecycle, lifecycle::quiesce, schedule::timer, settings, storage};

pub(crate) struct Lease<'a> {
    ptr: *mut driver::Driver,
    _borrow: marker::PhantomData<&'a mut driver::Driver>,
}

impl<'a> Lease<'a> {
    pub(crate) fn take(driver: pin::Pin<&'a mut driver::Driver>) -> Self {
        // SAFETY: Lease retains the exclusive pinned borrow until the scope returns.
        let ptr = unsafe { driver.get_unchecked_mut() };
        Self {
            ptr,
            _borrow: marker::PhantomData,
        }
    }

    pub(crate) fn run<R>(
        mut self,
        timer_cache_limit: settings::ScheduleCapacity,
        owner: quiesce::Lease,
        run: impl for<'d> FnOnce(lifecycle::Scope<'d>) -> R,
    ) -> R {
        brand::Token::scope_with_region(move |token, region| {
            let timer = pin::pin!(timer::Timer::with_capacity(timer_cache_limit, &region));
            // SAFETY: the pinned backing value surrounds `run`. Its generative
            // lifetime cannot occur in `R`, so no safe reference can escape.
            let timer = unsafe { bind(timer.as_ref()) };
            run(self.enter(token, region, timer.get_ref(), owner))
        })
    }

    pub(crate) fn run_with_storage<S, R>(
        self,
        timer_cache_limit: settings::ScheduleCapacity,
        owner: quiesce::Lease,
        factory: S,
        run: impl for<'d> FnOnce(lifecycle::Scope<'d>, pin::Pin<&'d S::Output<'d>>) -> R,
    ) -> Result<R, S::Error>
    where
        S: storage::Factory,
    {
        self.run(timer_cache_limit, owner, move |mut scope| {
            let value = {
                let mut context = storage::Context::new(scope.context());
                factory.build(&mut context)?
            };
            let storage = pin::pin!(value);
            // SAFETY: storage is pinned until `run` returns and its generative
            // lifetime cannot occur in `R`. It is dropped before the timer.
            let storage = unsafe { bind(storage.as_ref()) };
            Ok(run(scope, storage))
        })
    }

    fn enter<'d>(
        &mut self,
        token: brand::Token<'d>,
        region: region::Token<'d>,
        timer: &'d timer::Timer<'d>,
        owner: quiesce::Lease,
    ) -> lifecycle::Scope<'d> {
        use crate::driver::Reference;
        // SAFETY: the brand token scope bounds 'd, while Lease retains the pinned
        // exclusive borrow until that scope returns.
        let driver: &'d mut driver::Driver = unsafe { &mut *self.ptr };
        let reference = Reference::new(&driver.shared);
        lifecycle::Scope::new(
            reference,
            &mut driver.backend,
            &mut driver.flights,
            region,
            token,
            timer,
            owner,
        )
    }
}

unsafe fn bind<'d, T: ?Sized + 'd>(value: pin::Pin<&T>) -> pin::Pin<&'d T> {
    let ptr = ptr::from_ref(value.get_ref());
    // SAFETY: the caller guarantees that the pinned backing value remains live
    // and immovable for the entire generative lifetime.
    unsafe { pin::Pin::new_unchecked(&*ptr) }
}
