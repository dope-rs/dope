use std::cell::Cell;
use std::io;
use std::pin::{Pin, pin};

use dope_core::driver::control::ContextControl;
use dope_core::driver::ext::DriverExt;
use o3::cell::{BrandCell, BrandToken};

use super::run::Run;
use crate::driver::Config;
use crate::driver::Driver;
use crate::hash::Seed;
use crate::runtime::__private::RootTask;
use crate::runtime::dispatcher::Dispatcher;
use crate::{DriverContext, DriverRef};
use std::ptr::from_ref;

pub trait StorageFactory: 'static {
    type Output<'d>: 'd;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d>;
}

impl StorageFactory for () {
    type Output<'d> = ();

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {}
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
        driver.as_mut().scope(move |mut access, token| {
            let storage = storage.build(&mut access.reborrow());
            let storage = pin!(storage);
            let mut core = SessionCore {
                driver: access,
                seed,
                token,
            };
            f(Session {
                storage: unsafe { extend_storage(storage.as_ref()) },
                core: &mut core,
            })
        })
    }
}

pub struct Session<'scope, 'd: 'scope, S = ()> {
    storage: Pin<&'d S>,
    core: &'scope mut SessionCore<'d>,
}

unsafe fn extend_storage<'d, S>(storage: Pin<&S>) -> Pin<&'d S> {
    unsafe { Pin::new_unchecked(&*from_ref(storage.get_ref())) }
}

struct SessionCore<'d> {
    driver: DriverContext<'d, 'd>,
    seed: Seed,
    token: BrandToken<'d>,
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
        Dispatcher::shutdown(
            self.cell.borrow_pin_mut(&mut core.token),
            &mut core.driver.reborrow(),
        );
    }
}

impl Drop for SessionCore<'_> {
    fn drop(&mut self) {
        self.driver.prepare_drop();
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
        (self.storage_pin(), self.core.driver.reborrow())
    }

    #[doc(hidden)]
    pub fn token_and_driver(&mut self) -> (&mut BrandToken<'d>, DriverContext<'_, 'd>) {
        let core = &mut *self.core;
        (&mut core.token, core.driver.reborrow())
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.core.driver.driver_ref()
    }

    #[doc(hidden)]
    pub fn driver_access(&mut self) -> DriverContext<'_, 'd> {
        self.core.driver.reborrow()
    }

    pub const fn seed(&self) -> Seed {
        self.core.seed
    }

    pub fn token(&mut self) -> &mut BrandToken<'d> {
        &mut self.core.token
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
        Run::new(
            &mut self.core.driver,
            dispatcher,
            &mut self.core.token,
            None,
        )
        .block_on(root)
    }

    pub fn run<D: Dispatcher<'d>>(&mut self, dispatcher: Pin<&BrandCell<'d, D>>) -> io::Result<()> {
        Run::new(
            &mut self.core.driver,
            dispatcher,
            &mut self.core.token,
            None,
        )
        .run()
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
        Run::new(
            &mut core.driver,
            self.cell,
            &mut core.token,
            Some(self.shutdown),
        )
        .block_on(root)
    }

    pub fn run(&mut self) -> io::Result<()> {
        let core = &mut *self.session.core;
        Run::new(
            &mut core.driver,
            self.cell,
            &mut core.token,
            Some(self.shutdown),
        )
        .run()
    }
}
