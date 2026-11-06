use std::{cell::Cell, ops, os::fd, task::Poll};

use dope_core::{
    dope_sys::thread::signal::Signal as _,
    driver::{
        life::creation,
        schedule::ready::wake::{self, store::Slot},
        settings,
    },
    platform::signal,
};

use crate::{
    executor::{
        self, create,
        drive::{self, clock, turn},
        root, session,
    },
    shutdown,
};

pub type Error = <signal::Source<'static> as dope_core::dope_sys::thread::signal::Signal>::Error;

/// Owns the process signal across executor generations.
pub struct Signal<'process> {
    source: signal::Source<'process>,
    result: Cell<Option<Result<(), Error>>>,
}

impl<'process> Signal<'process> {
    pub const fn new(source: signal::Source<'process>) -> Self {
        Self {
            source,
            result: Cell::new(None),
        }
    }

    pub fn result(&self) -> Option<Result<(), Error>> {
        self.result.get()
    }
}

impl fd::AsFd for Signal<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.source.as_fd()
    }
}

impl executor::Startup for &mut Signal<'_> {
    fn installed(&self) -> Result<(), executor::InstallError> {
        Ok(())
    }

    fn observed(&self, _request: shutdown::Requested<Self>) {
        if self.result.get().is_none() {
            self.result.set(Some(self.source.wait()));
        }
    }
}

impl<'source, 'process> executor::Build for &'source mut Signal<'process> {
    type Shutdown = Self;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<executor::Executor<(), Self>, create::Error<'_>> {
        executor::Executor::new(attempt, config)?
            .shutdown(self)
            .map_err(create::Error::from)
    }
}

/// One reborrowable waiter inside its live application.
pub struct Wait<'signal, 'd> {
    result: &'signal Cell<Option<Result<(), Error>>>,
    wake: &'signal Slot<'d>,
}

impl<'d> Wait<'_, 'd> {
    pub fn poll(&mut self, wake: wake::Waker<'d>) -> Poll<Result<(), Error>> {
        match self.result.get() {
            Some(result) => Poll::Ready(result),
            None => {
                self.wake.set(wake);
                Poll::Pending
            }
        }
    }
}

/// Keeps the root alive to perform its own shutdown cleanup.
pub struct App<'signal, 'app, 'life: 'app, 'd: 'life, D: executor::raw::Layout<'d>, Q> {
    access: session::Access<'app, 'life, 'd, D, Q>,
    wake: &'signal Slot<'d>,
}

impl<'app, 'life: 'app, 'd: 'life, 'source, 'process, D>
    session::App<'app, 'life, 'd, D, &'source mut Signal<'process>>
where
    D: executor::raw::App<'d>,
{
    pub fn with_signal<R>(
        self,
        run: impl for<'signal> FnOnce(
            App<'signal, 'app, 'life, 'd, D, &'source mut Signal<'process>>,
            Wait<'signal, 'd>,
        ) -> R,
    ) -> R {
        let wake = Slot::empty();
        let result = &self.access.core.lifecycle.result;
        run(
            App {
                access: self.access,
                wake: &wake,
            },
            Wait {
                result,
                wake: &wake,
            },
        )
    }
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> App<'_, 'app, 'life, 'd, D, Q>
where
    D: executor::raw::App<'d>,
    Q: executor::Startup,
{
    pub fn drive<R>(&mut self, root: R) -> Result<R::Output, drive::Error>
    where
        R: root::Root<'d>,
    {
        let source = self.access.core.lifecycle;
        let wake = self.wake;
        self.access
            .reborrow()
            .drive_with(clock::wall(), root, drive::tick, || {
                source.observed(shutdown::Requested::new());
                wake.wake_once();
                turn::Status::Active
            })
    }
}

impl<'app, 'life: 'app, 'd: 'life, D: executor::raw::Layout<'d>, Q> ops::Deref
    for App<'_, 'app, 'life, 'd, D, Q>
{
    type Target = session::Access<'app, 'life, 'd, D, Q>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'d, D: executor::raw::Layout<'d>, Q> ops::DerefMut for App<'_, '_, '_, 'd, D, Q> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
