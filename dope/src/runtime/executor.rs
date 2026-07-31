use std::cell::Cell;
use std::io;
use std::pin::{Pin, pin};
use std::ptr::from_ref;

use dope_core::driver::Scope;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ext::DriverExt;
use o3::cell::{BrandCell, BrandToken};

use super::run::Run;
use crate::driver::Config;
use crate::driver::Driver;
use crate::hash::Seed;
use crate::runtime::__private::RootTask;
use crate::runtime::dispatcher::{Dispatcher, FinishContext};
use crate::{DriverContext, DriverRef};

pub trait StorageFactory: 'static {
    type Output<'d>: 'd;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d>;
}

#[doc(hidden)]
#[repr(transparent)]
pub struct ValueStorage<T>(T);

impl StorageFactory for () {
    type Output<'d> = ();

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {}
}

impl<T: 'static> StorageFactory for ValueStorage<T> {
    type Output<'d> = T;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        self.0
    }
}

impl<A: StorageFactory, B: StorageFactory> StorageFactory for (A, B) {
    type Output<'d> = (A::Output<'d>, B::Output<'d>);

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let first = self.0.build(&mut driver.reborrow());
        let second = self.1.build(driver);
        (first, second)
    }
}

pub struct Executor<S = ()> {
    storage: S,
    driver: Driver,
    seed: Seed,
}

impl Executor<()> {
    pub fn new(cfg: Config) -> io::Result<Self> {
        Self::with_seed(cfg, Seed::random()?)
    }

    pub fn with_seed(cfg: Config, seed: Seed) -> io::Result<Self> {
        let driver = Driver::new(cfg)?;
        Ok(Self {
            storage: (),
            driver,
            seed,
        })
    }
}

impl<S> Executor<S> {
    pub fn with_storage<T: 'static>(self, storage: T) -> Executor<ValueStorage<T>> {
        Executor {
            storage: ValueStorage(storage),
            driver: self.driver,
            seed: self.seed,
        }
    }

    pub fn with_storage_factory<T: StorageFactory>(self, storage: T) -> Executor<T> {
        Executor {
            storage,
            driver: self.driver,
            seed: self.seed,
        }
    }
}

impl<S: StorageFactory> Executor<S> {
    pub fn enter<R>(
        self,
        f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd, S::Output<'d>>) -> R,
    ) -> R {
        let Self {
            storage,
            driver,
            seed,
        } = self;
        let mut driver = pin!(driver);
        driver.as_mut().scope(move |mut scope| {
            let storage = storage.build(&mut scope.context());
            let storage = pin!(storage);
            let mut core = SessionCore { scope, seed };
            // SAFETY: `storage` is pinned before this reference is formed and is
            // dropped only after `f` returns. Both lifetimes are universally
            // quantified by `enter`, so neither the session nor a storage
            // reference can escape through `R`. This binds the pinned storage
            // to the same generative scope as the driver, enabling storage that
            // contains zero-copy handles into its own pinned pools.
            let storage = unsafe { Pin::new_unchecked(&*from_ref(storage.as_ref().get_ref())) };
            f(Session {
                storage,
                core: &mut core,
            })
        })
    }
}

pub struct Session<'scope, 'd: 'scope, S = ()> {
    storage: Pin<&'d S>,
    core: &'scope mut SessionCore<'d>,
}

struct SessionCore<'d> {
    scope: Scope<'d>,
    seed: Seed,
}

pub struct AppSession<'a, 'scope, 'd: 'scope, S, D> {
    session: &'a mut Session<'scope, 'd, S>,
    cell: Pin<&'a BrandCell<'d, D>>,
    shutdown: &'a Cell<bool>,
}

struct AppScope<'a, 'scope, 'd: 'scope, S, D>
where
    D: Dispatcher<'d>,
{
    session: &'a mut Session<'scope, 'd, S>,
    cell: Pin<&'a BrandCell<'d, D>>,
    shutdown: Cell<bool>,
}

impl<'scope, 'd: 'scope, S, D> Drop for AppScope<'_, 'scope, 'd, S, D>
where
    D: Dispatcher<'d>,
{
    fn drop(&mut self) {
        if self.shutdown.replace(true) {
            return;
        }
        let core = &mut *self.session.core;
        let (token, mut driver) = core.scope.token_and_context();
        Dispatcher::shutdown(self.cell.borrow_pin_mut(token), &mut driver);
        let mut context = FinishContext::new(driver.reborrow());
        Dispatcher::finish(self.cell.borrow_pin_mut(token), &mut context);
    }
}

impl Drop for SessionCore<'_> {
    fn drop(&mut self) {
        self.scope.context().prepare_drop();
    }
}

impl<'scope, 'd: 'scope, S> Session<'scope, 'd, S> {
    pub fn storage(&self) -> &'d S
    where
        S: 'd,
    {
        self.storage.get_ref()
    }

    pub fn storage_pin(&self) -> Pin<&'d S>
    where
        S: 'd,
    {
        self.storage
    }

    pub fn storage_and_driver(&mut self) -> (Pin<&'d S>, DriverContext<'_, 'd>)
    where
        S: 'd,
    {
        (self.storage_pin(), self.core.scope.context())
    }

    #[doc(hidden)]
    pub fn token_and_driver(&mut self) -> (&mut BrandToken<'d>, DriverContext<'_, 'd>) {
        self.core.scope.token_and_context()
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.core.scope.driver_ref()
    }

    #[doc(hidden)]
    pub fn driver_access(&mut self) -> DriverContext<'_, 'd> {
        self.core.scope.context()
    }

    pub const fn seed(&self) -> Seed {
        self.core.seed
    }

    pub fn token(&mut self) -> &mut BrandToken<'d> {
        self.core.scope.token()
    }

    pub fn with_app<D, R>(
        &mut self,
        app: D,
        f: impl for<'a> FnOnce(AppSession<'a, 'scope, 'd, S, D>) -> R,
    ) -> R
    where
        D: Dispatcher<'d>,
    {
        let cell = pin!(BrandCell::new(app));
        let scope = AppScope {
            session: self,
            cell: cell.as_ref(),
            shutdown: Cell::new(false),
        };
        f(AppSession {
            session: scope.session,
            cell: scope.cell,
            shutdown: &scope.shutdown,
        })
    }

    #[doc(hidden)]
    pub fn block_on_with<D, R, T>(
        &mut self,
        dispatcher: Pin<&BrandCell<'d, D>>,
        root: R,
    ) -> io::Result<T>
    where
        D: Dispatcher<'d>,
        R: RootTask<'d, T>,
    {
        let (token, mut driver) = self.core.scope.token_and_context();
        Run::new(&mut driver, dispatcher, token, None).block_on(root)
    }

    pub fn run<D: Dispatcher<'d>>(&mut self, dispatcher: Pin<&BrandCell<'d, D>>) -> io::Result<()> {
        let (token, mut driver) = self.core.scope.token_and_context();
        Run::new(&mut driver, dispatcher, token, None).run()
    }
}

impl<'a, 'scope, 'd: 'scope, S, D> AppSession<'a, 'scope, 'd, S, D>
where
    D: Dispatcher<'d>,
{
    #[doc(hidden)]
    pub fn driver(&self) -> DriverRef<'d> {
        self.session.driver()
    }

    #[doc(hidden)]
    pub fn block_on_with<R, T>(&mut self, root: R) -> io::Result<T>
    where
        R: RootTask<'d, T>,
    {
        let core = &mut *self.session.core;
        let (token, mut driver) = core.scope.token_and_context();
        Run::new(&mut driver, self.cell, token, Some(self.shutdown)).block_on(root)
    }

    pub fn run(&mut self) -> io::Result<()> {
        let core = &mut *self.session.core;
        let (token, mut driver) = core.scope.token_and_context();
        Run::new(&mut driver, self.cell, token, Some(self.shutdown)).run()
    }
}
