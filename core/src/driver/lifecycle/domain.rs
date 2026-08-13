use std::{io, mem, os::fd, pin};

use crate::{
    backend,
    driver::{
        self,
        lifecycle::{self, quiesce},
        storage,
    },
    platform,
};

/// Driver and every external source retained by its backend.
#[doc(hidden)]
pub struct Domain<Q = ()> {
    driver: driver::Driver,
    source: Q,
}

/// Authority to register a shutdown descriptor retained by a [`Domain`].
#[repr(transparent)]
pub(crate) struct Source<'source>(fd::BorrowedFd<'source>);

const _: () = {
    assert!(mem::size_of::<Domain<()>>() == mem::size_of::<driver::Driver>());
    assert!(mem::size_of::<Source<'static>>() == mem::size_of::<fd::BorrowedFd<'static>>());
    assert!(mem::align_of::<Source<'static>>() == mem::align_of::<fd::BorrowedFd<'static>>());
};

impl<'source> Source<'source> {
    fn new(descriptor: fd::BorrowedFd<'source>) -> Self {
        Self(descriptor)
    }

    pub(crate) fn into_fd(self) -> fd::BorrowedFd<'source> {
        self.0
    }
}

impl Domain<()> {
    pub fn new(driver: driver::Driver) -> Self {
        Self { driver, source: () }
    }

    pub fn fd<Q>(
        self,
        source: Q,
        select: impl for<'a> FnOnce(&'a Q) -> fd::BorrowedFd<'a>,
    ) -> io::Result<Domain<Q>> {
        let Self { driver, source: () } = self;
        let mut domain = Domain { driver, source };
        let source = Source::new(select(&domain.source));
        <backend::Backend as platform::Runtime>::register_shutdown(
            &mut domain.driver.backend,
            source,
        )?;
        Ok(domain)
    }
}

impl<Q> Domain<Q> {
    pub fn enter<S, R>(
        self,
        owner: quiesce::Lease,
        factory: S,
        run: impl for<'d> FnOnce(lifecycle::Scope<'d>, pin::Pin<&'d S::Output<'d>>, &Q) -> R,
    ) -> Result<R, S::Error>
    where
        S: storage::Factory,
    {
        let Self { driver, source } = self;
        let output = {
            let mut driver = pin::pin!(driver);
            driver
                .as_mut()
                .scope_with_storage(owner, factory, |scope, storage| {
                    run(scope, storage, &source)
                })
        };
        drop(source);
        output
    }
}
